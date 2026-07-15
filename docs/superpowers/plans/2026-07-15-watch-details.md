# ZooKeeper AdminServer Watch Details Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the authenticated AdminServer command watch_details/wchd for bounded, filterable local Watch registration details enriched with active client connection metadata.

**Architecture:** Add a bounded registration-query API to both WatchManager implementations and expose Data/Children delegates through DataTree. The command snapshots active plain and TLS connections, narrows candidate sessions, queries at most limit + 1 registrations, projects them into JSON fields, and maps validation or manager failures to the specified HTTP status.

**Tech Stack:** Java 17, ZooKeeper server internals, Jetty AdminServer, JUnit 5, Mockito, Maven, Checkstyle, SpotBugs.

## Global Constraints

- The command names are watch_details and wchd, and results cover only the current ZooKeeper Server.
- path, session_id, client_ip, and limit are optional AND filters; path and IP matching are exact.
- limit defaults to 100 and must be between 1 and 1000; collection stops at limit + 1 and total_count is not computed.
- Data and Children Watch registrations are both returned; modes are standard, persistent, and persistent_recursive.
- Only non-stale ServerCnxn instances with nonzero Session IDs and active remote addresses are returned.
- Duplicate active connections for one Session ID use the greatest connection_established_at.
- The command requires ZooDefs.Perms.ALL on root path /.
- Watch registration/trigger hot paths and per-Watch resident memory are unchanged.
- WatchManagerOptimized must not call getWatcher2PathesMap().
- ServerCnxn#getSessionTimeout() becomes public; no weakly typed connection-info Map is used.
- Every production behavior is introduced only after its focused test has failed for the expected missing behavior.

---

### Task 1: Immutable registration model and standard WatchManager query

**Files:**
- Create: zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchRegistration.java
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/IWatchManager.java
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManager.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/watch/WatchManagerTest.java

**Interfaces:**
- Consumes: WatchManager watchTable/watch2Paths, WatchStats#hasMode(WatcherMode), ServerCnxn#getSessionId(), ServerCnxn#isStale().
- Produces: WatchRegistration(String, long, WatcherMode), getPath(), getSessionId(), getWatcherMode(), and IWatchManager#getWatchRegistrations(String, Set<Long>, int).

- [ ] **Step 1: Write failing standard-manager tests**

Add Mockito imports and AtomicBoolean to WatchManagerTest, then add these tests:

~~~java
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.concurrent.atomic.AtomicBoolean;

@Test
public void testGetWatchRegistrationsExpandsModesAndFilters() {
    WatchManager manager = new WatchManager();
    ServerCnxn active = mock(ServerCnxn.class);
    when(active.getSessionId()).thenReturn(0x11L);
    when(active.isStale()).thenReturn(false);

    ServerCnxn otherSession = mock(ServerCnxn.class);
    when(otherSession.getSessionId()).thenReturn(0x22L);
    when(otherSession.isStale()).thenReturn(false);

    AtomicBoolean stale = new AtomicBoolean(false);
    ServerCnxn staleConnection = mock(ServerCnxn.class);
    when(staleConnection.getSessionId()).thenReturn(0x11L);
    when(staleConnection.isStale()).thenAnswer(invocation -> stale.get());

    manager.addWatch("/node1", active, WatcherMode.STANDARD);
    manager.addWatch("/node1", active, WatcherMode.PERSISTENT);
    manager.addWatch("/node1", active, WatcherMode.PERSISTENT_RECURSIVE);
    manager.addWatch("/node2", active, WatcherMode.STANDARD);
    manager.addWatch("/node1", otherSession, WatcherMode.STANDARD);
    manager.addWatch("/node1", staleConnection, WatcherMode.STANDARD);
    stale.set(true);

    List<WatchRegistration> registrations = manager.getWatchRegistrations(
        "/node1",
        java.util.Collections.singleton(0x11L),
        10
    );

    assertEquals(3, registrations.size());
    Set<WatcherMode> modes = new HashSet<>();
    for (WatchRegistration registration : registrations) {
        assertEquals("/node1", registration.getPath());
        assertEquals(0x11L, registration.getSessionId());
        modes.add(registration.getWatcherMode());
    }
    assertEquals(
        new HashSet<>(java.util.Arrays.asList(
            WatcherMode.STANDARD,
            WatcherMode.PERSISTENT,
            WatcherMode.PERSISTENT_RECURSIVE
        )),
        modes
    );
}

@Test
public void testGetWatchRegistrationsStopsAtMaxResults() {
    WatchManager manager = new WatchManager();
    ServerCnxn connection = mock(ServerCnxn.class);
    when(connection.getSessionId()).thenReturn(0x33L);
    when(connection.isStale()).thenReturn(false);
    manager.addWatch("/node1", connection);
    manager.addWatch("/node2", connection);

    List<WatchRegistration> registrations =
        manager.getWatchRegistrations(null, null, 1);

    assertEquals(1, registrations.size());
}
~~~

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=WatchManagerTest#testGetWatchRegistrationsExpandsModesAndFilters+testGetWatchRegistrationsStopsAtMaxResults" test
~~~

Expected: compilation fails because WatchRegistration and getWatchRegistrations do not exist.

- [ ] **Step 3: Add the model, interface default, and standard implementation**

Create WatchRegistration.java:

~~~java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to You under
 * the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.watch;

/**
 * A single Watch registration held by an {@link IWatchManager}.
 */
public final class WatchRegistration {

    private final String path;
    private final long sessionId;
    private final WatcherMode watcherMode;

    /**
     * Creates a Watch registration.
     *
     * @param path watched znode path
     * @param sessionId owning Session ID
     * @param watcherMode Watch registration mode
     */
    public WatchRegistration(String path, long sessionId, WatcherMode watcherMode) {
        this.path = path;
        this.sessionId = sessionId;
        this.watcherMode = watcherMode;
    }

    /**
     * @return watched znode path
     */
    public String getPath() {
        return path;
    }

    /**
     * @return owning Session ID
     */
    public long getSessionId() {
        return sessionId;
    }

    /**
     * @return Watch registration mode
     */
    public WatcherMode getWatcherMode() {
        return watcherMode;
    }
}
~~~

Add java.util.Set import and this documented default method to IWatchManager:

~~~java
/**
 * Returns bounded Watch registration details.
 *
 * @param path exact path filter, or null for every path
 * @param sessionIds Session ID filter, or null for every session
 * @param maxResults maximum registrations to return
 * @return Watch registrations matching the filters
 */
default List<WatchRegistration> getWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    throw new UnsupportedOperationException(
        "Watch registration details are not supported"
    );
}
~~~

Add java.util.ArrayList and java.util.Set imports to WatchManager, then add:

~~~java
@Override
public synchronized List<WatchRegistration> getWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    if (maxResults <= 0) {
        return Collections.emptyList();
    }
    List<WatchRegistration> registrations =
        new ArrayList<>(Math.min(maxResults, 1024));
    if (path != null) {
        collectWatchRegistrations(
            path,
            watchTable.get(path),
            sessionIds,
            maxResults,
            registrations
        );
        return registrations;
    }
    for (Entry<String, Set<Watcher>> entry : watchTable.entrySet()) {
        if (collectWatchRegistrations(
                entry.getKey(),
                entry.getValue(),
                sessionIds,
                maxResults,
                registrations)) {
            break;
        }
    }
    return registrations;
}

private boolean collectWatchRegistrations(
        String path,
        Set<Watcher> watchers,
        Set<Long> sessionIds,
        int maxResults,
        List<WatchRegistration> registrations) {
    if (watchers == null) {
        return false;
    }
    for (Watcher watcher : watchers) {
        if (!(watcher instanceof ServerCnxn) || isDeadWatcher(watcher)) {
            continue;
        }
        long sessionId = ((ServerCnxn) watcher).getSessionId();
        if (sessionId == 0 || (sessionIds != null && !sessionIds.contains(sessionId))) {
            continue;
        }
        Map<String, WatchStats> paths = watch2Paths.get(watcher);
        WatchStats stats = paths == null ? null : paths.get(path);
        if (stats == null) {
            continue;
        }
        for (WatcherMode watcherMode : WatcherMode.values()) {
            if (stats.hasMode(watcherMode)) {
                registrations.add(new WatchRegistration(path, sessionId, watcherMode));
                if (registrations.size() >= maxResults) {
                    return true;
                }
            }
        }
    }
    return false;
}
~~~

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the command from Step 2.

Expected: both tests pass.

- [ ] **Step 5: Commit the standard query**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchRegistration.java zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/IWatchManager.java zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManager.java zookeeper-server/src/test/java/org/apache/zookeeper/server/watch/WatchManagerTest.java
git commit -m "实现普通 Watch 明细查询"
~~~

---

### Task 2: Bounded WatchManagerOptimized query

**Files:**
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManagerOptimized.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/watch/WatchManagerTest.java

**Interfaces:**
- Consumes: IWatchManager#getWatchRegistrations(String, Set<Long>, int), pathWatches, watcherBitIdMap.
- Produces: WatchManagerOptimized#getWatchRegistrations returning only WatcherMode.STANDARD without a reverse-index copy.

- [ ] **Step 1: Write the failing optimized-manager test**

~~~java
@Test
public void testOptimizedGetWatchRegistrationsFiltersAndStopsEarly() {
    WatchManagerOptimized manager = new WatchManagerOptimized();
    try {
        ServerCnxn active = mock(ServerCnxn.class);
        when(active.getSessionId()).thenReturn(0x44L);
        when(active.isStale()).thenReturn(false);

        AtomicBoolean stale = new AtomicBoolean(false);
        ServerCnxn staleConnection = mock(ServerCnxn.class);
        when(staleConnection.getSessionId()).thenReturn(0x44L);
        when(staleConnection.isStale()).thenAnswer(invocation -> stale.get());

        manager.addWatch("/node1", active);
        manager.addWatch("/node2", active);
        manager.addWatch("/node1", staleConnection);
        stale.set(true);

        List<WatchRegistration> registrations = manager.getWatchRegistrations(
            "/node1",
            java.util.Collections.singleton(0x44L),
            1
        );

        assertEquals(1, registrations.size());
        assertEquals("/node1", registrations.get(0).getPath());
        assertEquals(0x44L, registrations.get(0).getSessionId());
        assertEquals(WatcherMode.STANDARD, registrations.get(0).getWatcherMode());
    } finally {
        manager.shutdown();
    }
}
~~~

- [ ] **Step 2: Run the test and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=WatchManagerTest#testOptimizedGetWatchRegistrationsFiltersAndStopsEarly" test
~~~

Expected: the test fails with UnsupportedOperationException from the IWatchManager default method.

- [ ] **Step 3: Implement direct optimized traversal**

Add java.util.ArrayList and java.util.Collections imports, then add:

~~~java
@Override
public List<WatchRegistration> getWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    if (maxResults <= 0) {
        return Collections.emptyList();
    }
    List<WatchRegistration> registrations =
        new ArrayList<>(Math.min(maxResults, 1024));
    if (path != null) {
        collectWatchRegistrations(
            path,
            pathWatches.get(path),
            sessionIds,
            maxResults,
            registrations
        );
        return registrations;
    }
    for (Entry<String, BitHashSet> entry : pathWatches.entrySet()) {
        if (collectWatchRegistrations(
                entry.getKey(),
                entry.getValue(),
                sessionIds,
                maxResults,
                registrations)) {
            break;
        }
    }
    return registrations;
}

private boolean collectWatchRegistrations(
        String path,
        BitHashSet watchers,
        Set<Long> sessionIds,
        int maxResults,
        List<WatchRegistration> registrations) {
    if (watchers == null) {
        return false;
    }
    synchronized (watchers) {
        for (Integer watcherBit : watchers) {
            Watcher watcher = watcherBitIdMap.get(watcherBit);
            if (!(watcher instanceof ServerCnxn) || isDeadWatcher(watcher)) {
                continue;
            }
            long sessionId = ((ServerCnxn) watcher).getSessionId();
            if (sessionId == 0 || (sessionIds != null && !sessionIds.contains(sessionId))) {
                continue;
            }
            registrations.add(
                new WatchRegistration(path, sessionId, WatcherMode.STANDARD)
            );
            if (registrations.size() >= maxResults) {
                return true;
            }
        }
    }
    return false;
}
~~~

- [ ] **Step 4: Run WatchManagerTest and verify GREEN**

Run:

~~~powershell
mvn -pl zookeeper-server -Dtest=WatchManagerTest test
~~~

Expected: all WatchManagerTest cases pass and the optimized cleaner shuts down cleanly.

- [ ] **Step 5: Commit the optimized query**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/watch/WatchManagerOptimized.java zookeeper-server/src/test/java/org/apache/zookeeper/server/watch/WatchManagerTest.java
git commit -m "实现优化 Watch 明细查询"
~~~

---

### Task 3: DataTree Data/Children query delegates

**Files:**
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/DataTreeTest.java

**Interfaces:**
- Consumes: IWatchManager#getWatchRegistrations(String, Set<Long>, int).
- Produces: DataTree#getDataWatchRegistrations(String, Set<Long>, int) and DataTree#getChildWatchRegistrations(String, Set<Long>, int).

- [ ] **Step 1: Write the failing DataTree test**

Add Mockito imports, List/Set imports, and WatchRegistration/WatchManager
imports, then add:

~~~java
@Test
public void testGetDataAndChildWatchRegistrations() throws Exception {
    String property = org.apache.zookeeper.server.watch.WatchManagerFactory
        .ZOOKEEPER_WATCH_MANAGER_NAME;
    String previous = System.getProperty(property);
    System.setProperty(property, WatchManager.class.getName());
    DataTree dataTree;
    try {
        dataTree = new DataTree();
    } finally {
        if (previous == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, previous);
        }
    }

    try {
        ServerCnxn connection = mock(ServerCnxn.class);
        when(connection.getSessionId()).thenReturn(0x55L);
        when(connection.isStale()).thenReturn(false);

        dataTree.statNode("/", connection);
        dataTree.getChildren("/", null, connection);
        dataTree.addWatch(
            "/persistent",
            connection,
            ZooDefs.AddWatchModes.persistent
        );
        dataTree.addWatch(
            "/recursive",
            connection,
            ZooDefs.AddWatchModes.persistentRecursive
        );

        List<WatchRegistration> dataRegistrations =
            dataTree.getDataWatchRegistrations(null, null, 10);
        List<WatchRegistration> childRegistrations =
            dataTree.getChildWatchRegistrations(null, null, 10);

        Set<String> data = new java.util.HashSet<>();
        for (WatchRegistration registration : dataRegistrations) {
            data.add(registration.getPath() + ":" + registration.getWatcherMode());
        }
        Set<String> children = new java.util.HashSet<>();
        for (WatchRegistration registration : childRegistrations) {
            children.add(registration.getPath() + ":" + registration.getWatcherMode());
        }

        assertEquals(
            new java.util.HashSet<>(java.util.Arrays.asList(
                "/:STANDARD",
                "/persistent:PERSISTENT",
                "/recursive:PERSISTENT_RECURSIVE"
            )),
            data
        );
        assertEquals(
            new java.util.HashSet<>(java.util.Arrays.asList(
                "/:STANDARD",
                "/persistent:PERSISTENT"
            )),
            children
        );
    } finally {
        dataTree.shutdownWatcher();
    }
}
~~~

- [ ] **Step 2: Run the test and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=DataTreeTest#testGetDataAndChildWatchRegistrations" test
~~~

Expected: compilation fails because both DataTree query methods are missing.

- [ ] **Step 3: Add the two delegates**

Add WatchRegistration import to DataTree and insert:

~~~java
/**
 * Returns bounded Data Watch registrations.
 *
 * @param path exact path filter, or null for every path
 * @param sessionIds Session ID filter, or null for every session
 * @param maxResults maximum registrations to return
 * @return matching Data Watch registrations
 */
public List<WatchRegistration> getDataWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    return dataWatches.getWatchRegistrations(path, sessionIds, maxResults);
}

/**
 * Returns bounded Children Watch registrations.
 *
 * @param path exact path filter, or null for every path
 * @param sessionIds Session ID filter, or null for every session
 * @param maxResults maximum registrations to return
 * @return matching Children Watch registrations
 */
public List<WatchRegistration> getChildWatchRegistrations(
        String path,
        Set<Long> sessionIds,
        int maxResults) {
    return childWatches.getWatchRegistrations(path, sessionIds, maxResults);
}
~~~

- [ ] **Step 4: Run the focused DataTree test and verify GREEN**

Run the command from Step 2.

Expected: the test passes with three Data registrations and two Children registrations.

- [ ] **Step 5: Commit the delegates**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java zookeeper-server/src/test/java/org/apache/zookeeper/server/DataTreeTest.java
git commit -m "增加 DataTree Watch 明细入口"
~~~

---

### Task 4: Command registration, request validation, response envelope, and authorization

**Files:**
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandAuthTest.java

**Interfaces:**
- Consumes: PathUtils#validatePath(String), AuthRequest, CommandResponse.
- Produces: registered WatchDetailsCommand with primary name watch_details, alias wchd, parsed WatchDetailsQuery, and the specified 400/401/403 behavior.

- [ ] **Step 1: Write failing command contract and validation tests**

Add assertSame import plus java.util.Collections and
java.util.List to CommandsTest, then add:

~~~java
@Test
public void testWatchDetailsCommandRegistrationAndAuth() {
    Command primary = Commands.getCommand("watch_details");
    Command alias = Commands.getCommand("wchd");

    assertNotNull(primary);
    assertSame(primary, alias);
    assertEquals("watch_details", primary.getPrimaryName());
    assertEquals(ZooDefs.Perms.ALL, primary.getAuthRequest().getPermission());
    assertEquals(Commands.ROOT_PATH, primary.getAuthRequest().getPath());
}

@Test
public void testWatchDetailsValidationAndEmptyResponse() {
    ZooKeeperServer zkServer = mock(ZooKeeperServer.class);
    when(zkServer.getServerId()).thenReturn(7L);
    Commands.WatchDetailsCommand command = new Commands.WatchDetailsCommand();

    assertWatchDetailsBadRequest(command, zkServer, "path", "relative", "Invalid path: relative");
    assertWatchDetailsBadRequest(command, zkServer, "session_id", "0xxyz", "Invalid session_id: 0xxyz");
    assertWatchDetailsBadRequest(command, zkServer, "client_ip", "", "client_ip must not be empty");
    assertWatchDetailsBadRequest(command, zkServer, "limit", "abc", "limit must be an integer");
    assertWatchDetailsBadRequest(command, zkServer, "limit", "0", "limit must be between 1 and 1000");
    assertWatchDetailsBadRequest(command, zkServer, "limit", "1001", "limit must be between 1 and 1000");

    Map<String, String> valid = new HashMap<>();
    valid.put("path", "/");
    valid.put("session_id", "0xffffffffffffffff");
    valid.put("client_ip", "127.0.0.1");
    valid.put("limit", "1000");
    CommandResponse response = command.runGet(zkServer, valid);
    Map<String, Object> result = response.toMap();

    assertEquals(HttpServletResponse.SC_OK, response.getStatusCode());
    assertEquals(7L, result.get("server_id"));
    assertEquals(0, result.get("returned_count"));
    assertEquals(false, result.get("truncated"));
    assertTrue(((List<?>) result.get("watches")).isEmpty());

    CommandResponse minimumLimit = command.runGet(
        zkServer,
        java.util.Collections.singletonMap("limit", "1")
    );
    assertEquals(HttpServletResponse.SC_OK, minimumLimit.getStatusCode());
}

private void assertWatchDetailsBadRequest(
        Commands.WatchDetailsCommand command,
        ZooKeeperServer zkServer,
        String key,
        String value,
        String error) {
    CommandResponse response = command.runGet(
        zkServer,
        java.util.Collections.singletonMap(key, value)
    );
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatusCode());
    assertEquals(error, response.getError());
}
~~~

Add these authorization tests to CommandAuthTest:

~~~java
@Test
public void testWatchDetailsRequiresAuthorization() {
    CommandResponse response = Commands.runGetCommand(
        "watch_details",
        zks,
        Collections.emptyMap(),
        null,
        null
    );
    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, response.getStatusCode());
}

@Test
public void testWatchDetailsAuthorizationDeniedAndAllowed() throws Exception {
    setupRootACL(AuthSchema.DIGEST);
    try {
        CommandResponse denied = Commands.runGetCommand(
            "watch_details",
            zks,
            Collections.emptyMap(),
            buildInvalidAuthorizationForDigest(),
            null
        );
        assertEquals(HttpServletResponse.SC_FORBIDDEN, denied.getStatusCode());

        CommandResponse allowed = Commands.runGetCommand(
            "watch_details",
            zks,
            Collections.emptyMap(),
            buildAuthorizationForDigest(),
            null
        );
        assertEquals(HttpServletResponse.SC_OK, allowed.getStatusCode());
    } finally {
        addAuthInfoForDigest(zk);
        resetRootACL(zk);
    }
}
~~~

- [ ] **Step 2: Run the tests and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=CommandsTest#testWatchDetailsCommandRegistrationAndAuth+testWatchDetailsValidationAndEmptyResponse,CommandAuthTest#testWatchDetailsRequiresAuthorization+testWatchDetailsAuthorizationDeniedAndAllowed" test
~~~

Expected: compilation fails because WatchDetailsCommand is absent and neither command name is registered.

- [ ] **Step 3: Implement registration, parsing, envelope, and auth declaration**

Add PathUtils import, register new WatchDetailsCommand next to the other Watch commands, and add:

~~~java
public static class WatchDetailsCommand extends GetCommand {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MIN_LIMIT = 1;
    private static final int MAX_LIMIT = 1000;

    public WatchDetailsCommand() {
        super(
            Arrays.asList("watch_details", "wchd"),
            true,
            new AuthRequest(ZooDefs.Perms.ALL, ROOT_PATH)
        );
    }

    @Override
    public CommandResponse runGet(
            ZooKeeperServer zkServer,
            Map<String, String> kwargs) {
        final WatchDetailsQuery query;
        try {
            query = WatchDetailsQuery.parse(kwargs);
        } catch (IllegalArgumentException e) {
            return new CommandResponse(
                getPrimaryName(),
                e.getMessage(),
                HttpServletResponse.SC_BAD_REQUEST
            );
        }

        CommandResponse response = initializeResponse();
        response.put("server_id", zkServer.getServerId());
        response.put("returned_count", 0);
        response.put("truncated", false);
        response.put("watches", Collections.emptyList());
        return response;
    }

    private static final class WatchDetailsQuery {

        private final String path;
        private final Long sessionId;
        private final String clientIp;
        private final int limit;

        private WatchDetailsQuery(
                String path,
                Long sessionId,
                String clientIp,
                int limit) {
            this.path = path;
            this.sessionId = sessionId;
            this.clientIp = clientIp;
            this.limit = limit;
        }

        private static WatchDetailsQuery parse(Map<String, String> kwargs) {
            Map<String, String> params =
                kwargs == null ? Collections.emptyMap() : kwargs;
            String path = params.get("path");
            if (path != null) {
                try {
                    PathUtils.validatePath(path);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid path: " + path);
                }
            }

            Long sessionId = null;
            if (params.containsKey("session_id")) {
                String value = params.get("session_id");
                try {
                    if (value == null || value.isEmpty()) {
                        throw new NumberFormatException();
                    }
                    boolean hexadecimal =
                        value.startsWith("0x") || value.startsWith("0X");
                    String digits = hexadecimal ? value.substring(2) : value;
                    sessionId = Long.parseUnsignedLong(
                        digits,
                        hexadecimal ? 16 : 10
                    );
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Invalid session_id: " + value
                    );
                }
            }

            String clientIp = params.get("client_ip");
            if (params.containsKey("client_ip")
                    && (clientIp == null || clientIp.trim().isEmpty())) {
                throw new IllegalArgumentException(
                    "client_ip must not be empty"
                );
            }

            int limit = DEFAULT_LIMIT;
            if (params.containsKey("limit")) {
                String value = params.get("limit");
                try {
                    limit = Integer.parseInt(value);
                } catch (NumberFormatException | NullPointerException e) {
                    throw new IllegalArgumentException(
                        "limit must be an integer"
                    );
                }
                if (limit < MIN_LIMIT || limit > MAX_LIMIT) {
                    throw new IllegalArgumentException(
                        "limit must be between 1 and 1000"
                    );
                }
            }
            return new WatchDetailsQuery(path, sessionId, clientIp, limit);
        }
    }
}
~~~

- [ ] **Step 4: Run the command and auth tests and verify GREEN**

Run the command from Step 2.

Expected: both command tests and both authorization tests pass.

- [ ] **Step 5: Commit the command contract**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandAuthTest.java
git commit -m "增加 watch_details 命令契约"
~~~

---

### Task 5: Active connection snapshots, Watch association, filters, and truncation

**Files:**
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/ServerCnxn.java
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java

**Interfaces:**
- Consumes: DataTree query delegates, both ServerCnxnFactory instances, WatchRegistration, WatcherMode.
- Produces: public ServerCnxn#getSessionTimeout(), immutable ConnectionSnapshot, JSON Watch maps, latest-connection selection, AND filtering, and limit + 1 truncation.

- [ ] **Step 1: Add failing command-detail tests and helpers**

Add Date, InetSocketAddress, Set, AtomicBoolean, DataTree, ServerCnxn,
ZKDatabase, and ZooDefs imports to CommandsTest. Add these helpers:

~~~java
private ServerCnxn mockWatchConnection(
        long sessionId,
        String clientIp,
        int clientPort,
        long establishedAt,
        int sessionTimeout,
        boolean secure,
        AtomicBoolean stale) {
    ServerCnxn connection = mock(ServerCnxn.class);
    when(connection.getSessionId()).thenReturn(sessionId);
    when(connection.getHostAddress()).thenReturn(clientIp);
    when(connection.getRemoteSocketAddress()).thenReturn(
        new InetSocketAddress(clientIp, clientPort)
    );
    when(connection.getEstablished()).thenReturn(new Date(establishedAt));
    when(connection.getSessionTimeout()).thenReturn(sessionTimeout);
    when(connection.isSecure()).thenReturn(secure);
    when(connection.isStale()).thenAnswer(invocation -> stale.get());
    return connection;
}

private ZooKeeperServer mockWatchDetailsServer(
        DataTree dataTree,
        Iterable<ServerCnxn> plainConnections,
        Iterable<ServerCnxn> secureConnections) {
    ZooKeeperServer zkServer = mock(ZooKeeperServer.class);
    ZKDatabase database = mock(ZKDatabase.class);
    when(zkServer.getZKDatabase()).thenReturn(database);
    when(database.getDataTree()).thenReturn(dataTree);
    when(zkServer.getServerId()).thenReturn(9L);
    if (plainConnections != null) {
        ServerCnxnFactory factory = mock(ServerCnxnFactory.class);
        when(factory.getConnections()).thenReturn(plainConnections);
        when(zkServer.getServerCnxnFactory()).thenReturn(factory);
    }
    if (secureConnections != null) {
        ServerCnxnFactory factory = mock(ServerCnxnFactory.class);
        when(factory.getConnections()).thenReturn(secureConnections);
        when(zkServer.getSecureServerCnxnFactory()).thenReturn(factory);
    }
    return zkServer;
}

private DataTree newStandardDataTree() {
    String property = org.apache.zookeeper.server.watch.WatchManagerFactory
        .ZOOKEEPER_WATCH_MANAGER_NAME;
    String previous = System.getProperty(property);
    System.setProperty(
        property,
        org.apache.zookeeper.server.watch.WatchManager.class.getName()
    );
    try {
        return new DataTree();
    } finally {
        if (previous == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, previous);
        }
    }
}

@SuppressWarnings("unchecked")
private List<Map<String, Object>> watchDetails(CommandResponse response) {
    return (List<Map<String, Object>>) response.toMap().get("watches");
}
~~~

Add focused behavior tests:

~~~java
@Test
public void testWatchDetailsReturnsDataAndChildModesWithConnectionFields()
        throws Exception {
    DataTree dataTree = newStandardDataTree();
    try {
        AtomicBoolean stale = new AtomicBoolean(false);
        long sessionId = 0x8000000000000011L;
        ServerCnxn connection = mockWatchConnection(
            sessionId, "127.0.0.1", 21811, 123456L, 30000, true, stale
        );
        dataTree.statNode("/", connection);
        dataTree.getChildren("/", null, connection);
        dataTree.addWatch(
            "/persistent",
            connection,
            ZooDefs.AddWatchModes.persistent
        );
        dataTree.addWatch(
            "/recursive",
            connection,
            ZooDefs.AddWatchModes.persistentRecursive
        );

        ZooKeeperServer zkServer = mockWatchDetailsServer(
            dataTree,
            Collections.emptyList(),
            Collections.singletonList(connection)
        );
        Map<String, String> params = new HashMap<>();
        params.put("path", "/persistent");
        params.put("session_id", "0x8000000000000011");
        params.put("client_ip", "127.0.0.1");

        CommandResponse response =
            new Commands.WatchDetailsCommand().runGet(zkServer, params);
        List<Map<String, Object>> watches = watchDetails(response);

        assertEquals(2, watches.size());
        Set<String> kinds = new java.util.HashSet<>();
        for (Map<String, Object> watch : watches) {
            kinds.add((String) watch.get("watch_kind"));
            assertEquals("/persistent", watch.get("path"));
            assertEquals("0x8000000000000011", watch.get("session_id"));
            assertEquals("127.0.0.1", watch.get("client_ip"));
            assertEquals(21811, watch.get("client_port"));
            assertEquals("persistent", watch.get("watch_mode"));
            assertEquals(123456L, watch.get("connection_established_at"));
            assertEquals(30000, watch.get("session_timeout_ms"));
            assertEquals(true, watch.get("secure"));
        }
        assertEquals(
            new java.util.HashSet<>(java.util.Arrays.asList("data", "children")),
            kinds
        );
        assertEquals(2, response.toMap().get("returned_count"));
        assertEquals(false, response.toMap().get("truncated"));

        Map<String, String> allModeParams =
            Collections.singletonMap(
                "session_id",
                Long.toUnsignedString(sessionId)
            );
        List<Map<String, Object>> allWatches = watchDetails(
            new Commands.WatchDetailsCommand().runGet(
                zkServer,
                allModeParams
            )
        );
        Set<String> modes = new java.util.HashSet<>();
        for (Map<String, Object> watch : allWatches) {
            modes.add((String) watch.get("watch_mode"));
        }
        assertEquals(
            new java.util.HashSet<>(java.util.Arrays.asList(
                "standard",
                "persistent",
                "persistent_recursive"
            )),
            modes
        );
    } finally {
        dataTree.shutdownWatcher();
    }
}

@Test
public void testWatchDetailsUsesNewestConnectionAndFiltersInactiveWatches()
        throws Exception {
    DataTree dataTree = newStandardDataTree();
    try {
        AtomicBoolean activeState = new AtomicBoolean(false);
        ServerCnxn oldConnection = mockWatchConnection(
            0x66L, "10.0.0.1", 1111, 100L, 10000, false, activeState
        );
        ServerCnxn newConnection = mockWatchConnection(
            0x66L, "10.0.0.2", 2222, 200L, 20000, false, activeState
        );
        dataTree.statNode("/", oldConnection);

        AtomicBoolean staleState = new AtomicBoolean(false);
        ServerCnxn staleConnection = mockWatchConnection(
            0x77L, "10.0.0.3", 3333, 300L, 30000, false, staleState
        );
        dataTree.statNode("/", staleConnection);
        staleState.set(true);

        ServerCnxn disconnected = mockWatchConnection(
            0x88L,
            "10.0.0.4",
            4444,
            400L,
            40000,
            false,
            new AtomicBoolean(false)
        );
        dataTree.statNode("/", disconnected);

        ZooKeeperServer zkServer = mockWatchDetailsServer(
            dataTree,
            java.util.Arrays.asList(oldConnection, newConnection, staleConnection),
            null
        );
        Map<String, String> params = new HashMap<>();
        params.put("session_id", Long.toUnsignedString(0x66L));
        params.put("client_ip", "10.0.0.2");
        List<Map<String, Object>> watches = watchDetails(
            new Commands.WatchDetailsCommand().runGet(zkServer, params)
        );

        assertEquals(1, watches.size());
        assertEquals("0x66", watches.get(0).get("session_id"));
        assertEquals("10.0.0.2", watches.get(0).get("client_ip"));
        assertEquals(2222, watches.get(0).get("client_port"));
        assertEquals(200L, watches.get(0).get("connection_established_at"));
        assertEquals(20000, watches.get(0).get("session_timeout_ms"));
    } finally {
        dataTree.shutdownWatcher();
    }
}

@Test
public void testWatchDetailsDefaultLimitAndTruncation() {
    DataTree dataTree = newStandardDataTree();
    try {
        ServerCnxn connection = mockWatchConnection(
            0x99L,
            "127.0.0.1",
            9999,
            999L,
            30000,
            false,
            new AtomicBoolean(false)
        );
        for (int i = 0; i < 101; i++) {
            dataTree.addWatch(
                "/limit-" + i,
                connection,
                ZooDefs.AddWatchModes.persistentRecursive
            );
        }
        ZooKeeperServer zkServer = mockWatchDetailsServer(
            dataTree,
            Collections.singletonList(connection),
            null
        );

        CommandResponse truncated =
            new Commands.WatchDetailsCommand().runGet(
                zkServer,
                Collections.emptyMap()
            );
        assertEquals(100, watchDetails(truncated).size());
        assertEquals(100, truncated.toMap().get("returned_count"));
        assertEquals(true, truncated.toMap().get("truncated"));

        CommandResponse complete =
            new Commands.WatchDetailsCommand().runGet(
                zkServer,
                Collections.singletonMap("limit", "1000")
            );
        assertEquals(101, watchDetails(complete).size());
        assertEquals(false, complete.toMap().get("truncated"));
    } finally {
        dataTree.shutdownWatcher();
    }
}
~~~

- [ ] **Step 2: Run the three tests and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=CommandsTest#testWatchDetailsReturnsDataAndChildModesWithConnectionFields+testWatchDetailsUsesNewestConnectionAndFiltersInactiveWatches+testWatchDetailsDefaultLimitAndTruncation" test
~~~

Expected: compilation fails because ServerCnxn#getSessionTimeout() is not public, then after exposing only that method the assertions still fail because the command returns an empty list.

- [ ] **Step 3: Expose the timeout and implement snapshot/query projection**

Change the ServerCnxn declaration to the documented public API:

~~~java
/**
 * @return negotiated Session Timeout in milliseconds
 */
public abstract int getSessionTimeout();
~~~

Add ArrayList, LinkedHashMap, Locale, InetSocketAddress, ServerCnxn, WatchRegistration, and WatcherMode imports to Commands. Replace the success portion of WatchDetailsCommand#runGet with:

~~~java
Map<Long, ConnectionSnapshot> connections =
    snapshotConnections(zkServer);
Set<Long> candidateSessionIds = new HashSet<>();
for (Map.Entry<Long, ConnectionSnapshot> entry : connections.entrySet()) {
    if (query.sessionId != null
            && entry.getKey().longValue() != query.sessionId.longValue()) {
        continue;
    }
    if (query.clientIp != null
            && !query.clientIp.equals(entry.getValue().clientIp)) {
        continue;
    }
    candidateSessionIds.add(entry.getKey());
}

int maxResults = query.limit + 1;
List<Map<String, Object>> watches = new ArrayList<>(maxResults);
if (!candidateSessionIds.isEmpty()) {
    DataTree dataTree = zkServer.getZKDatabase().getDataTree();
    appendWatchDetails(
        dataTree.getDataWatchRegistrations(
            query.path,
            candidateSessionIds,
            maxResults
        ),
        "data",
        connections,
        watches,
        maxResults
    );
    if (watches.size() < maxResults) {
        appendWatchDetails(
            dataTree.getChildWatchRegistrations(
                query.path,
                candidateSessionIds,
                maxResults - watches.size()
            ),
            "children",
            connections,
            watches,
            maxResults
        );
    }
}

boolean truncated = watches.size() > query.limit;
if (truncated) {
    watches = new ArrayList<>(watches.subList(0, query.limit));
}
CommandResponse response = initializeResponse();
response.put("server_id", zkServer.getServerId());
response.put("returned_count", watches.size());
response.put("truncated", truncated);
response.put("watches", watches);
return response;
~~~

Add these members to WatchDetailsCommand:

~~~java
private static Map<Long, ConnectionSnapshot> snapshotConnections(
        ZooKeeperServer zkServer) {
    Map<Long, ConnectionSnapshot> snapshots = new HashMap<>();
    addConnectionSnapshots(zkServer.getServerCnxnFactory(), snapshots);
    addConnectionSnapshots(
        zkServer.getSecureServerCnxnFactory(),
        snapshots
    );
    return snapshots;
}

private static void addConnectionSnapshots(
        ServerCnxnFactory factory,
        Map<Long, ConnectionSnapshot> snapshots) {
    if (factory == null) {
        return;
    }
    for (ServerCnxn connection : factory.getConnections()) {
        if (connection == null || connection.isStale()) {
            continue;
        }
        long sessionId = connection.getSessionId();
        String clientIp = connection.getHostAddress();
        InetSocketAddress remoteAddress =
            connection.getRemoteSocketAddress();
        if (sessionId == 0
                || clientIp == null
                || clientIp.isEmpty()
                || remoteAddress == null) {
            continue;
        }
        ConnectionSnapshot candidate = new ConnectionSnapshot(
            sessionId,
            clientIp,
            remoteAddress.getPort(),
            connection.getEstablished().getTime(),
            connection.getSessionTimeout(),
            connection.isSecure()
        );
        if (connection.isStale()) {
            continue;
        }
        ConnectionSnapshot current = snapshots.get(sessionId);
        if (current == null
                || candidate.establishedAt > current.establishedAt) {
            snapshots.put(sessionId, candidate);
        }
    }
}

private static void appendWatchDetails(
        List<WatchRegistration> registrations,
        String watchKind,
        Map<Long, ConnectionSnapshot> connections,
        List<Map<String, Object>> watches,
        int maxResults) {
    for (WatchRegistration registration : registrations) {
        ConnectionSnapshot connection =
            connections.get(registration.getSessionId());
        if (connection == null) {
            continue;
        }
        Map<String, Object> watch = new LinkedHashMap<>();
        watch.put("path", registration.getPath());
        watch.put(
            "session_id",
            "0x" + Long.toHexString(connection.sessionId)
        );
        watch.put("client_ip", connection.clientIp);
        watch.put("client_port", connection.clientPort);
        watch.put("watch_kind", watchKind);
        watch.put(
            "watch_mode",
            registration.getWatcherMode().name().toLowerCase(Locale.ROOT)
        );
        watch.put(
            "connection_established_at",
            connection.establishedAt
        );
        watch.put("session_timeout_ms", connection.sessionTimeout);
        watch.put("secure", connection.secure);
        watches.add(watch);
        if (watches.size() >= maxResults) {
            return;
        }
    }
}

private static final class ConnectionSnapshot {

    private final long sessionId;
    private final String clientIp;
    private final int clientPort;
    private final long establishedAt;
    private final int sessionTimeout;
    private final boolean secure;

    private ConnectionSnapshot(
            long sessionId,
            String clientIp,
            int clientPort,
            long establishedAt,
            int sessionTimeout,
            boolean secure) {
        this.sessionId = sessionId;
        this.clientIp = clientIp;
        this.clientPort = clientPort;
        this.establishedAt = establishedAt;
        this.sessionTimeout = sessionTimeout;
        this.secure = secure;
    }
}
~~~

- [ ] **Step 4: Run command, DataTree, and WatchManager tests and verify GREEN**

Run:

~~~powershell
mvn -pl zookeeper-server -Dtest=CommandsTest,DataTreeTest,WatchManagerTest test
~~~

Expected: all selected tests pass.

- [ ] **Step 5: Commit connection association**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/ServerCnxn.java zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java
git commit -m "关联 Watch 与客户端连接"
~~~

---

### Task 6: Unsupported manager and unexpected failure mapping

**Files:**
- Modify: zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java
- Test: zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java

**Interfaces:**
- Consumes: UnsupportedOperationException from the IWatchManager default method and RuntimeException from query dependencies.
- Produces: HTTP 501 with the configured-manager message and HTTP 500 with the generic failure message.

- [ ] **Step 1: Write failing error-mapping tests**

~~~java
@Test
public void testWatchDetailsMapsUnsupportedManagerTo501() {
    DataTree dataTree = mock(DataTree.class);
    when(dataTree.getDataWatchRegistrations(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anySet(),
            org.mockito.ArgumentMatchers.anyInt()))
        .thenThrow(new UnsupportedOperationException());
    ServerCnxn connection = mockWatchConnection(
        0xaaL,
        "127.0.0.1",
        1234,
        1L,
        30000,
        false,
        new AtomicBoolean(false)
    );
    ZooKeeperServer zkServer = mockWatchDetailsServer(
        dataTree,
        Collections.singletonList(connection),
        null
    );

    CommandResponse response =
        new Commands.WatchDetailsCommand().runGet(
            zkServer,
            Collections.emptyMap()
        );

    assertEquals(HttpServletResponse.SC_NOT_IMPLEMENTED, response.getStatusCode());
    assertEquals(
        "Watch details are not supported by the configured WatchManager",
        response.getError()
    );
}

@Test
public void testWatchDetailsMapsUnexpectedFailureTo500() {
    DataTree dataTree = mock(DataTree.class);
    when(dataTree.getDataWatchRegistrations(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anySet(),
            org.mockito.ArgumentMatchers.anyInt()))
        .thenThrow(new IllegalStateException("internal detail"));
    ServerCnxn connection = mockWatchConnection(
        0xbbL,
        "127.0.0.1",
        1234,
        1L,
        30000,
        false,
        new AtomicBoolean(false)
    );
    ZooKeeperServer zkServer = mockWatchDetailsServer(
        dataTree,
        Collections.singletonList(connection),
        null
    );

    CommandResponse response =
        new Commands.WatchDetailsCommand().runGet(
            zkServer,
            Collections.emptyMap()
        );

    assertEquals(
        HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        response.getStatusCode()
    );
    assertEquals("Failed to query watch details", response.getError());
}
~~~

- [ ] **Step 2: Run the tests and verify RED**

Run:

~~~powershell
mvn -pl zookeeper-server "-Dtest=CommandsTest#testWatchDetailsMapsUnsupportedManagerTo501+testWatchDetailsMapsUnexpectedFailureTo500" test
~~~

Expected: both tests error because the exceptions currently escape runGet.

- [ ] **Step 3: Add narrow exception translation**

Keep request parsing before this block. Move snapshot, query, projection, and success response creation into a private execute method with the exact signature:

~~~java
private CommandResponse execute(
        ZooKeeperServer zkServer,
        WatchDetailsQuery query)
~~~

Call it from runGet with:

~~~java
try {
    return execute(zkServer, query);
} catch (UnsupportedOperationException e) {
    return new CommandResponse(
        getPrimaryName(),
        "Watch details are not supported by the configured WatchManager",
        HttpServletResponse.SC_NOT_IMPLEMENTED
    );
} catch (RuntimeException e) {
    LOG.warn("Failed to query watch details", e);
    return new CommandResponse(
        getPrimaryName(),
        "Failed to query watch details",
        HttpServletResponse.SC_INTERNAL_SERVER_ERROR
    );
}
~~~

Implement execute with the complete success path:

~~~java
private CommandResponse execute(
        ZooKeeperServer zkServer,
        WatchDetailsQuery query) {
    Map<Long, ConnectionSnapshot> connections =
        snapshotConnections(zkServer);
    Set<Long> candidateSessionIds = new HashSet<>();
    for (Map.Entry<Long, ConnectionSnapshot> entry
            : connections.entrySet()) {
        if (query.sessionId != null
                && entry.getKey().longValue()
                    != query.sessionId.longValue()) {
            continue;
        }
        if (query.clientIp != null
                && !query.clientIp.equals(entry.getValue().clientIp)) {
            continue;
        }
        candidateSessionIds.add(entry.getKey());
    }

    int maxResults = query.limit + 1;
    List<Map<String, Object>> watches = new ArrayList<>(maxResults);
    if (!candidateSessionIds.isEmpty()) {
        DataTree dataTree = zkServer.getZKDatabase().getDataTree();
        appendWatchDetails(
            dataTree.getDataWatchRegistrations(
                query.path,
                candidateSessionIds,
                maxResults
            ),
            "data",
            connections,
            watches,
            maxResults
        );
        if (watches.size() < maxResults) {
            appendWatchDetails(
                dataTree.getChildWatchRegistrations(
                    query.path,
                    candidateSessionIds,
                    maxResults - watches.size()
                ),
                "children",
                connections,
                watches,
                maxResults
            );
        }
    }

    boolean truncated = watches.size() > query.limit;
    if (truncated) {
        watches = new ArrayList<>(
            watches.subList(0, query.limit)
        );
    }
    CommandResponse response = initializeResponse();
    response.put("server_id", zkServer.getServerId());
    response.put("returned_count", watches.size());
    response.put("truncated", truncated);
    response.put("watches", watches);
    return response;
}
~~~

- [ ] **Step 4: Run all AdminServer command tests and verify GREEN**

Run:

~~~powershell
mvn -pl zookeeper-server -Dtest=CommandsTest,CommandAuthTest test
~~~

Expected: all selected tests pass; logs contain the expected warning only for the deliberate 500 test.

- [ ] **Step 5: Commit error mapping**

~~~powershell
git add -- zookeeper-server/src/main/java/org/apache/zookeeper/server/admin/Commands.java zookeeper-server/src/test/java/org/apache/zookeeper/server/admin/CommandsTest.java
git commit -m "完善 Watch 明细错误处理"
~~~

---

### Task 7: AdminServer documentation and full verification

**Files:**
- Modify: zookeeper-website/app/pages/_docs/docs/_mdx/admin-ops/administrators-guide/commands.mdx
- Verify: all files modified in Tasks 1–6

**Interfaces:**
- Consumes: final command parameters, response fields, permission and node-local behavior.
- Produces: user-facing AdminServer command documentation and verification evidence for tests, Checkstyle, and SpotBugs.

- [ ] **Step 1: Document the command**

Insert this command entry after watch_summary/wchs:

~~~markdown
- _watch_details/wchd_ :
  Returns Watch registration details for the current ZooKeeper server and
  associates each result with its active client connection.
  Requires AdminServer authentication and all permissions on the root znode.
  Optional query parameters are "path" (exact znode path), "session_id"
  (decimal or hexadecimal), "client_ip" (exact address), and "limit"
  (1 through 1000, default 100). Multiple filters are combined with AND.
  Returns "server_id", "returned_count", "truncated", and "watches".
  Each Watch entry contains "path", "session_id", "client_ip", "client_port",
  "watch_kind", "watch_mode", "connection_established_at",
  "session_timeout_ms", and "secure".
  Results are local to the queried server, are not ordered, and may reflect
  concurrent Watch or connection changes during the query.
~~~

- [ ] **Step 2: Run focused tests**

Run:

~~~powershell
mvn -pl zookeeper-server -Dtest=WatchManagerTest,DataTreeTest,CommandsTest,CommandAuthTest test
~~~

Expected: Maven exits 0 with no test failures.

- [ ] **Step 3: Run the full zookeeper-server test suite**

Run:

~~~powershell
mvn -pl zookeeper-server test
~~~

Expected: Maven exits 0 with BUILD SUCCESS and no failed tests.

- [ ] **Step 4: Run Checkstyle**

Run:

~~~powershell
mvn -pl zookeeper-server -DskipTests checkstyle:check
~~~

Expected: Maven exits 0 with BUILD SUCCESS and no Checkstyle violations.

- [ ] **Step 5: Run SpotBugs**

Run:

~~~powershell
mvn -pl zookeeper-server -DskipTests spotbugs:check
~~~

Expected: Maven exits 0 with BUILD SUCCESS and no SpotBugs violations.

- [ ] **Step 6: Inspect the final scope**

Run:

~~~powershell
git diff --check
git status --short
git diff --stat upstream/master...HEAD
~~~

Expected: diff check emits no errors; status contains only files intentionally modified for watch_details.

- [ ] **Step 7: Commit documentation or any verification-only formatting corrections**

~~~powershell
git add -- zookeeper-website/app/pages/_docs/docs/_mdx/admin-ops/administrators-guide/commands.mdx
git commit -m "补充 watch_details 命令文档"
~~~

If Checkstyle required formatting corrections, stage only the already in-scope files corrected in this task and include them in the same commit.
