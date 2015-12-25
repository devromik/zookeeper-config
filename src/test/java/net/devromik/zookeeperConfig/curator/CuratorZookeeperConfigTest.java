package net.devromik.zookeeperConfig.curator;

import java.util.*;
import org.junit.*;
import static com.google.common.collect.Sets.newHashSet;
import net.devromik.zookeeperConfig.*;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.*;
import static net.devromik.zookeeperConfig.ZookeeperConfig.CONNECTION_STRING_JVM_PROPERTY_NAME;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.internal.util.collections.Sets.newSet;

public class CuratorZookeeperConfigTest {

    final long TIMEOUT = SECONDS.toMillis(90L);

    // ****************************** //

    @Before
    public void beforeTest() throws Exception {
        zookeeperCluster = new TestZookeeperCluster();
        zookeeperCluster.init();

        System.setProperty(CONNECTION_STRING_JVM_PROPERTY_NAME, zookeeperCluster.getConnectionString());
        zookeeperConfig = new CuratorZookeeperConfig();
        zookeeperConfig.init();
    }

    @After
    public void afterTest() throws Exception {
        zookeeperConfig.destroy();
        zookeeperCluster.destroy();
    }

    // ****************************** //

    @Test
    public void complexTest() throws Exception {
        final long startTime = currentTimeMillis();

        assertFalse(zookeeperConfig.nodeExists("/complexTest/nodeWithEmptyData/nextLevelNode"));
        zookeeperConfig.createNode("/complexTest/nodeWithEmptyData/nextLevelNode");

        Collection<CuratorZookeeperConfig> zookeeperConfigs = new ArrayList<>();
        Collection<TestZookeeperNodeListener> subtreeListeners = new ArrayList<>();
        Collection<TestZookeeperNodeListener> nodeChildrenListeners = new ArrayList<>();

        for (ZookeeperInstance instance : ZookeeperInstance.values()) {
            CuratorZookeeperConfig zookeeperConfig = new CuratorZookeeperConfig(instance.getConnectionString());
            zookeeperConfig.init();
            zookeeperConfigs.add(zookeeperConfig);

            TestZookeeperNodeListener subtreeListener_1 = new TestZookeeperNodeListener();
            zookeeperConfig.addSubtreeListener("/complexTest", subtreeListener_1);
            subtreeListeners.add(subtreeListener_1);

            TestZookeeperNodeListener nodeChildrenListener_1 = new TestZookeeperNodeListener();
            zookeeperConfig.addNodeChildrenListener("/complexTest", nodeChildrenListener_1);
            nodeChildrenListeners.add(nodeChildrenListener_1);
        }

        /* ***** Node data is empty. ***** */

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithEmptyData/nextLevelNode",
            subtreeListeners,
            newSet(
                new NodePathAndData("/complexTest", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData/nextLevelNode", "")),
            startTime);

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithEmptyData/nextLevelNode",
            nodeChildrenListeners,
            newSet(new NodePathAndData("/complexTest/nodeWithEmptyData", "")),
            startTime);

        for (ZookeeperConfig zookeeperConfig : zookeeperConfigs) {
            TestZookeeperNodeListener subtreeListener_2 = new TestZookeeperNodeListener();
            zookeeperConfig.addSubtreeListener("/complexTest", subtreeListener_2);
            subtreeListeners.add(subtreeListener_2);

            TestZookeeperNodeListener nodeChildrenListener_2 = new TestZookeeperNodeListener();
            zookeeperConfig.addNodeChildrenListener("/complexTest", nodeChildrenListener_2);
            nodeChildrenListeners.add(nodeChildrenListener_2);
        }

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithEmptyData/nextLevelNode",
            subtreeListeners,
            newSet(
                new NodePathAndData("/complexTest", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData/nextLevelNode", "")),
            startTime);

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithEmptyData/nextLevelNode",
            nodeChildrenListeners,
            newSet(new NodePathAndData("/complexTest/nodeWithEmptyData", "")),
            startTime);

        assertTrue(zookeeperConfig.nodeExists("/complexTest/nodeWithEmptyData"));
        assertThat(zookeeperConfig.getString("/complexTest/nodeWithEmptyData"), is(""));

        zookeeperConfig.deleteNode("/complexTest/nodeWithEmptyData/nextLevelNode");
        zookeeperConfig.deleteNode("/complexTest/nodeWithEmptyData");

        checkThatChangePropagated(
            "delete",
            "/complexTest/nodeWithEmptyData",
            subtreeListeners,
            newSet(
                "/complexTest/nodeWithEmptyData/nextLevelNode",
                "/complexTest/nodeWithEmptyData"),
            startTime);

        checkThatChangePropagated(
            "delete",
            "/complexTest/nodeWithEmptyData",
            nodeChildrenListeners,
            newSet("/complexTest/nodeWithEmptyData"),
            startTime);

        assertFalse(zookeeperConfig.nodeExists("/complexTest/nodeWithEmptyData"));

        /* ***** Node data is non-empty. ***** */

        assertFalse(zookeeperConfig.nodeExists("/complexTest/nodeWithNonEmptyData"));
        zookeeperConfig.createNode("/complexTest/nodeWithNonEmptyData/nextLevelNode", "1");

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithNonEmptyData",
            subtreeListeners,
            newSet(
                new NodePathAndData("/complexTest", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData", ""),
                new NodePathAndData("/complexTest/nodeWithEmptyData/nextLevelNode", ""),
                new NodePathAndData("/complexTest/nodeWithNonEmptyData", "1"),
                new NodePathAndData("/complexTest/nodeWithNonEmptyData/nextLevelNode", "")),
            startTime);

        checkThatChangePropagated(
            "create",
            "/complexTest/nodeWithNonEmptyData",
            nodeChildrenListeners,
            newSet(
                new NodePathAndData("/complexTest/nodeWithEmptyData", ""),
                new NodePathAndData("/complexTest/nodeWithNonEmptyData", "1")),
            startTime);

        assertTrue(zookeeperConfig.nodeExists("/complexTest/nodeWithNonEmptyData/nextLevelNode"));
        assertThat(zookeeperConfig.getInt("/complexTest/nodeWithNonEmptyData/nextLevelNode"), is(1));

        zookeeperConfig.updateNodeData("/complexTest/nodeWithNonEmptyData", "2");

        checkThatChangePropagated(
            "update",
            "/complexTest/nodeWithNonEmptyData",
            subtreeListeners,
            newSet(new NodePathAndData("/complexTest/nodeWithNonEmptyData", "2")),
            startTime);

        checkThatChangePropagated(
            "update",
            "/complexTest/nodeWithNonEmptyData",
            nodeChildrenListeners,
            newSet(new NodePathAndData("/complexTest/nodeWithNonEmptyData", "2")),
            startTime);

        zookeeperConfig.deleteNode("/complexTest/nodeWithNonEmptyData/nextLevelNode");
        zookeeperConfig.deleteNode("/complexTest/nodeWithNonEmptyData");

        checkThatChangePropagated(
            "delete",
            "/complexTest/nodeWithNonEmptyData",
            subtreeListeners,
            newSet(
                "/complexTest/nodeWithEmptyData/nextLevelNode",
                "/complexTest/nodeWithEmptyData",
                "/complexTest/nodeWithNonEmptyData/nextLevelNode",
                "/complexTest/nodeWithNonEmptyData"),
            startTime);

        checkThatChangePropagated(
            "delete",
            "/complexTest/nodeWithNonEmptyData",
            nodeChildrenListeners,
            newSet(
                "/complexTest/nodeWithEmptyData",
                "/complexTest/nodeWithNonEmptyData"),
            startTime);

        assertFalse(zookeeperConfig.nodeExists("/complexTest/nodeWithNonEmptyData"));

        for (CuratorZookeeperConfig zookeeperConfig : zookeeperConfigs) {
            zookeeperConfig.destroy();
        }

        zookeeperConfig.deleteNode("/complexTest");
        assertFalse(zookeeperConfig.nodeExists("/complexTest"));
    }

    @Test(expected = ZookeeperException.class)
    public void cannotCreateNode_When_NodeAlreadyExists() {
        String nodePath = "/cannotCreateNode_When_NodeAlreadyExists";

        try {
            zookeeperConfig.createNode(nodePath);
            zookeeperConfig.createNode(nodePath);
        }
        finally {
            zookeeperConfig.deleteNode(nodePath);
        }
    }

    @Test
    public void canProvideNodeChildNames() throws Exception {
        zookeeperConfig.createNode("/canProvideNodeChildNames/child_1");
        zookeeperConfig.createNode("/canProvideNodeChildNames/child_2");

        Set<String> childPaths = newHashSet(zookeeperConfig.getNodeChildNames("/canProvideNodeChildNames"));
        assertThat(childPaths, is(newHashSet("child_1", "child_2")));

        zookeeperConfig.deleteNode("/canProvideNodeChildNames/child_1");
        childPaths = newHashSet(zookeeperConfig.getNodeChildNames("/canProvideNodeChildNames"));
        assertThat(childPaths, is(newHashSet("child_2")));

        zookeeperConfig.deleteNode("/canProvideNodeChildNames/child_2");
        childPaths = newHashSet(zookeeperConfig.getNodeChildNames("/canProvideNodeChildNames"));
        assertTrue(childPaths.isEmpty());

        zookeeperConfig.deleteNode("/canProvideNodeChildNames");
    }

    @Test
    public void canExecuteTransaction() throws Exception {
        ZookeeperTransaction transaction = new ZookeeperTransactionBuilder().
            createNode("/canExecuteTransaction", "").
            createNode("/canExecuteTransaction/child_1", "data_1").
            delete("/canExecuteTransaction/child_1").
            createNode("/canExecuteTransaction/child_2", "data_2").
        build();

        zookeeperConfig.executeTransaction(transaction);

        HashSet<String> childPaths = newHashSet(zookeeperConfig.getNodeChildNames("/canExecuteTransaction"));
        assertThat(childPaths, is(newHashSet("child_2")));
        assertThat(zookeeperConfig.getString("/canExecuteTransaction/child_2"), is("data_2"));

        zookeeperConfig.deleteNode("/canExecuteTransaction/child_2");
        zookeeperConfig.deleteNode("/canExecuteTransaction");
    }

    @Test
    public void doesNotDoTransactionByHalves() throws Exception {
        zookeeperConfig.createNode("/doesNotDoTransactionByHalves/child_1");
        zookeeperConfig.createNode("/doesNotDoTransactionByHalves/child_2");

        ZookeeperTransaction transaction = new ZookeeperTransactionBuilder().
            delete("/doesNotDoTransactionByHalves/child_1").
            delete("/doesNotDoTransactionByHalves/child_2").
            createNode("/doesNotDoTransactionByHalves/child_3", "").
        build();

        transaction.addOp(new NPETransactionOp("/doesNorDoTransactionByHalves/child_4", ""));

        try {
            zookeeperConfig.executeTransaction(transaction);
        }
        catch (ZookeeperException exception) {
            // expected
        }
        finally {
            HashSet<String> childPaths = newHashSet(zookeeperConfig.getNodeChildNames("/doesNotDoTransactionByHalves"));
            assertThat(childPaths, is(newHashSet("child_1", "child_2")));
            zookeeperConfig.deleteNode("/doesNotDoTransactionByHalves");
        }
    }

    // ****************************** //

    private void checkThatChangePropagated(
        String changeName,
        String nodePath,
        Collection<TestZookeeperNodeListener> listeners,
        Object expectedChangeResult,
        long startTime) throws Exception {

        boolean changePropagated;

        do {
            waitForChangePropagated();
            changePropagated = true;

            for (TestZookeeperNodeListener listener : listeners) {
                Object actual = null;

                switch (changeName) {
                    case "create":
                        actual = newHashSet(listener.getAddedNodes()); break;
                    case "update":
                        actual = newHashSet(listener.getUpdatedNodes()); break;
                    case "delete":
                        actual = newHashSet(listener.getDeletedNodePaths()); break;
                }

                assertNotNull(actual);

                if (!actual.equals(expectedChangeResult)) {
                    changePropagated = false;
                    break;
                }
            }
        }
        while (!changePropagated && !isTimeoutExpired(startTime));

        if (isTimeoutExpired(startTime)) {
            fail("Failed to " + changeName + " node \"" + nodePath + "\"");
        }
    }

    private void waitForChangePropagated() throws InterruptedException {
        sleep(50L);
    }

    private boolean isTimeoutExpired(long startTime) {
        return (currentTimeMillis() - startTime) > TIMEOUT;
    }

    // ****************************** //

    private TestZookeeperCluster zookeeperCluster;
    private CuratorZookeeperConfig zookeeperConfig;
}
