# ZooKeeper AdminServer `watch_details` 设计说明

## 目标与范围

为 AdminServer 新增只读命令 `watch_details`，别名为 `wchd`。命令查询当前 ZooKeeper Server 节点上的 Watch 注册明细，并关联当前有效客户端连接；不进行集群汇总，也不记录或返回 Watch 注册时间、持续时间、触发次数等当前服务端未维护的信息。

命令支持可选参数 `path`、`session_id`、`client_ip` 和 `limit`。多个过滤条件为 AND 关系。默认返回上限为 100，允许范围为 1～1000。

## 已确认的实现选择

采用类型安全的直接访问方案：将 `ServerCnxn#getSessionTimeout()` 从包级抽象方法提升为 `public`。`NIOServerCnxn` 和 `NettyServerCnxn` 的现有实现已经是 `public`，因此该调整只扩大基类 API 的可见性，不改变连接运行逻辑。

不通过 `ServerCnxn#getConnectionInfo(false)` 的弱类型 `Map` 读取 Session Timeout，也不新增跨包连接 DTO。

## 内部查询模型

新增不可变类型 `org.apache.zookeeper.server.watch.WatchRegistration`，仅保存：

- `String path`
- `long sessionId`
- `WatcherMode watcherMode`

该类型不持有 `ServerCnxn`，也不包含客户端地址等连接状态。

在 `IWatchManager` 增加默认方法：

```java
default List<WatchRegistration> getWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    throw new UnsupportedOperationException(
        "Watch registration details are not supported"
    );
}
```

默认方法保留第三方 `IWatchManager` 实现的二进制兼容性。自定义实现未覆盖该方法时，AdminServer 命令返回 HTTP 501。

## WatchManager 查询

`WatchManager#getWatchRegistrations` 在现有同步边界内读取 `watchTable` 和 `watch2Paths`：

1. `path` 非空时只读取 `watchTable.get(path)`。
2. `path` 为空时遍历 `watchTable`。
3. 仅处理 `ServerCnxn`，并过滤 stale、Session ID 为 0 和不在候选 `sessionIds` 中的连接。
4. 从 `watch2Paths` 获取路径对应的 `WatchStats`。
5. 按 `WatcherMode.values()` 检查每种模式，将组合状态拆成独立 `WatchRegistration`。
6. 收集到 `maxResults` 后立即停止。

查询不创建完整 Watcher-to-Path 副本。普通实现的底层集合不是并发集合，因此查询需要沿用同步访问以避免并发修改；路径过滤和 `maxResults` 用于尽早结束遍历。

## WatchManagerOptimized 查询

`WatchManagerOptimized#getWatchRegistrations` 直接遍历 `pathWatches` 和 `watcherBitIdMap`：

1. `path` 非空时只读取指定的 `BitHashSet`；否则弱一致遍历 `ConcurrentHashMap`。
2. 遍历单个 `BitHashSet` 时沿用现有同步方式。
3. 仅处理非 stale、Session ID 非 0 且属于候选会话的 `ServerCnxn`。
4. 每条结果的模式固定为 `WatcherMode.STANDARD`。
5. 达到 `maxResults` 后立即停止。

实现不得调用 `getWatcher2PathesMap()`，避免在 Watch 数量很大时构造全量反向索引。

## DataTree 查询入口

`DataTree` 增加两个只读委托方法：

```java
public List<WatchRegistration> getDataWatchRegistrations(
        String path, Set<Long> sessionIds, int maxResults)

public List<WatchRegistration> getChildWatchRegistrations(
        String path, Set<Long> sessionIds, int maxResults)
```

二者分别调用 `dataWatches` 和 `childWatches`。现有 `getWatches()`、`getWatchesByPath()` 只覆盖 Data Watch，不能复用。

## 命令处理流程

在 `Commands.java` 新增并注册 `WatchDetailsCommand extends GetCommand`：

```java
super(
    Arrays.asList("watch_details", "wchd"),
    true,
    new AuthRequest(ZooDefs.Perms.ALL, ROOT_PATH)
);
```

执行顺序如下：

1. 解析并校验请求参数。
2. 从普通和 TLS `ServerCnxnFactory` 收集连接。
3. 过滤 stale、Session ID 为 0、远端地址为空或 IP 为空的连接。
4. 将连接复制为命令私有的不可变 `ConnectionSnapshot`；相同 Session ID 保留 `connection_established_at` 最大的一条。
5. 根据 `session_id` 和 `client_ip` 缩小候选 Session ID 集合。
6. 先查询 Data Watch，再用剩余容量查询 Children Watch，总收集上限为 `limit + 1`。
7. 将 `WatchRegistration` 与 `ConnectionSnapshot` 关联；无法关联活动连接的注册不返回。
8. 若收集数量超过 `limit`，截取前 `limit` 条并设置 `truncated=true`。
9. 返回 `server_id`、`returned_count`、`truncated` 和 `watches`。

命令不保证结果顺序。JSON 中的每条 Watch 记录包含：

- `path`
- `session_id`，格式为 `0x` 加小写十六进制数字
- `client_ip`
- `client_port`
- `watch_kind`：`data` 或 `children`
- `watch_mode`：`standard`、`persistent` 或 `persistent_recursive`
- `connection_established_at`
- `session_timeout_ms`
- `secure`

Persistent Watch 在 Data 与 Children 管理器中各有注册，因此可返回两条记录。Persistent Recursive Watch 只来自 Data Watch。

## 参数与错误处理

- `path`：使用 ZooKeeper 路径校验规则进行完整路径精确匹配；非法值返回 HTTP 400，错误为 `Invalid path: ...`。
- `session_id`：以 `0x` 或 `0X` 开头时使用无符号十六进制解析，否则按十进制解析；格式错误返回 HTTP 400，错误为 `Invalid session_id: ...`。
- `client_ip`：只与 `ServerCnxn#getHostAddress()` 精确比较，不解析 DNS，不比较端口；空字符串返回 HTTP 400，错误为 `client_ip must not be empty`。
- `limit`：缺省为 100；非整数返回 HTTP 400，错误为 `limit must be an integer`；不在 1～1000 时返回 HTTP 400，错误为 `limit must be between 1 and 1000`。
- WatchManager 不支持明细查询：返回 HTTP 501，错误为 `Watch details are not supported by the configured WatchManager`。
- 未预期异常：记录服务端日志，返回 HTTP 500，错误为 `Failed to query watch details`，不向调用方暴露内部异常详情。
- 无匹配结果：返回 HTTP 200、空列表、`returned_count=0`、`truncated=false`。

认证缺失、认证失败和根节点 `/` 的 `ALL` ACL 权限不足，继续由 AdminServer 公共认证授权流程分别处理为 401 或 403。

## 并发与性能

命令提供诊断用途的弱一致结果，不承诺单点时间快照。连接信息只在命令开始时读取一次，JSON 序列化阶段不再访问活动 `ServerCnxn`。

实现必须保持以下约束：

- 不修改 Watch 注册、触发、删除和 Session 清理热路径。
- 不增加每条 Watch 的常驻字段。
- 不计算 `total_count`。
- 不创建全量 Watch 反向索引。
- 指定 `path` 时直接读取目标路径。
- 最多收集 `limit + 1` 条注册后停止。
- 普通与优化 WatchManager 的读取遵守各自现有并发模型，不抛出并发修改异常。

## 测试设计

测试按由内到外的 TDD 顺序覆盖：

1. `WatchRegistration` 值访问与两种 WatchManager 的查询过滤、模式拆分、stale 过滤、路径过滤、Session 过滤和数量上限。
2. `DataTree` 的 Data/Children Watch 委托，包含 Standard、Persistent、Persistent Recursive 以及同路径多模式。
3. `WatchDetailsCommand` 的无参数、全部合法参数、非法参数、多条件 AND、普通连接、TLS 连接、相同 Session 最新连接、无活动连接过滤、`limit + 1` 截断、501 和 500。
4. 命令注册与 `wchd` 别名。
5. AdminServer 公共认证流程下的 401、403 和授权成功。
6. 现有目标测试、模块测试、Checkstyle 和 SpotBugs。

测试不得依赖返回顺序；比较结果时按字段查找或转换为集合。

## 文件范围

主要修改：

- `zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/ServerCnxn.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/IWatchManager.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManager.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManagerOptimized.java`
- `zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchRegistration.java`
- `zookeeper-website/app/pages/_docs/docs/_mdx/admin-ops/administrators-guide/commands.mdx`

测试优先扩展现有文件：

- `zookeeper-server/src/test/java/org/apache/zookeeper/server/watch/WatchManagerTest.java`
- `zookeeper-server/src/test/java/org/apache/zookeeper/server/DataTreeTest.java`
- `zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java`
- `zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandAuthTest.java`

如命令测试夹具过大，可新增聚焦于 `watch_details` 的测试类，但不重构无关测试。

## 验收标准

完成后应满足原设计文档第一版验收标准：命令及别名可用，四类过滤参数有效，普通/TLS 连接可关联，Data/Children 与三种 Watch Mode 正确，Session ID 使用十六进制字符串，stale 或无活动连接的 Watch 不返回，限制和截断正确，认证授权生效，不增加 Watch 注册时间等未支持字段，不做集群汇总，并通过相关单元测试、Checkstyle 和 SpotBugs。
