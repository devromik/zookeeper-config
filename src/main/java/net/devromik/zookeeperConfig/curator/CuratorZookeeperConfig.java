package net.devromik.zookeeperConfig.curator;

import java.util.*;
import javax.annotation.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.*;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import static com.google.common.base.Charsets.*;
import static java.lang.Integer.*;
import static java.util.concurrent.TimeUnit.*;
import static net.devromik.slf4jUtils.Slf4jUtils.logException;
import net.devromik.zookeeperConfig.*;
import static org.apache.curator.framework.CuratorFrameworkFactory.*;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.*;
import static org.apache.curator.utils.ZKPaths.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.slf4j.LoggerFactory.*;

/**
 * @author Shulnyaev Roman
 */
public class CuratorZookeeperConfig implements ZookeeperConfig {

    /**
     * Connection string will be get as the JVM property
     * ZookeeperConfig.CONNECTION_STRING_JVM_PROPERTY_NAME value
     * within the init() method.
     */
    public CuratorZookeeperConfig() {}

    public CuratorZookeeperConfig(String connectionString) {
        this.connectionString = connectionString;
    }

    @PostConstruct
    public synchronized void init() throws Exception {
        if (connectionString == null) {
            connectionString = System.getProperty(CONNECTION_STRING_JVM_PROPERTY_NAME, DEFAULT_CONNECTION_STRING);
        }

        client = newClient(connectionString, new ExponentialBackoffRetry(1000, 5));
        client.start();

        if (!client.blockUntilConnected(10, SECONDS)) {
            throw new
                ZookeeperException(
                    "Could not connect to Zookeeper using the connection string \"" + connectionString + "\"");
        }
    }

    @PreDestroy
    public synchronized void destroy() throws Exception {
        closeSubtreeCaches();
        closeNodeChildrenCaches();
        closeClient();
    }

    private void closeClient() {
        client.close();
    }

    // ****************************** //

    @Override
    public synchronized boolean nodeExists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    // ****************************** //

    @Override
    public synchronized void createNode(String path) {
        createNode(path, "");
    }

    @Override
    public synchronized void createNode(String path, String data) {
        try {
            client.create().creatingParentsIfNeeded().withMode(PERSISTENT).forPath(path, data.getBytes());
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    @Override
    public synchronized void ensureNode(String path) {
        try {
            if (!nodeExists(path)) {
                createNode(path);
            }
        }
        catch (ZookeeperException exception) {
            if (!(exception.getCause() instanceof KeeperException.NodeExistsException)) {
                logException(logger, exception);
                throw exception;
            }
        }
    }

    // ****************************** //

    @Override
    public synchronized void updateNodeData(String path, String data) {
        try {
            client.setData().forPath(path, data.getBytes());
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    @Override
    public synchronized void deleteNode(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    // ****************************** //

    @Override
    public synchronized void executeTransaction(ZookeeperTransaction transaction) {
        if (transaction.isEmpty()) {
            return;
        }

        try {
            CuratorTransaction curatorTransaction = client.inTransaction();

            for (ZookeeperOp op : transaction) {
                String path = op.getNodePath();

                if (op instanceof CreateNodeOp) {
                    byte[] data = ((CreateNodeOp)op).getNodeData().getBytes(UTF_8);
                    curatorTransaction = curatorTransaction.create().forPath(path, data).and();
                }
                else if (op instanceof DeleteNodeOp) {
                    curatorTransaction = curatorTransaction.delete().forPath(path).and();
                }
            }

            ((CuratorTransactionFinal)curatorTransaction).commit();
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    // ****************************** //

    @Override
    public synchronized String getString(String path) {
        return new String(getData(path));
    }

    private byte[] getData(String path) {
        try {
            return client.getData().forPath(path);
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    @Override
    public synchronized int getInt(String path) {
        return parseInt(getString(path));
    }

    // ****************************** //

    @Override
    public synchronized void addSubtreeListener(String subtreeRootNodePath, final ZookeeperNodeListener listener) {
        ensureNode(subtreeRootNodePath);

        TreeCache subtreeCache =
            subtreeCaches.containsKey(subtreeRootNodePath) ?
            subtreeCaches.get(subtreeRootNodePath) :
            new TreeCache(client, subtreeRootNodePath);

        subtreeCache.getListenable().addListener(
            (curatorFramework, event) -> {
                 TreeCacheEvent.Type eventType = event.getType();
                 ChildData descendant = event.getData();

                 if (eventType == TreeCacheEvent.Type.NODE_ADDED) {
                     DateTime creationTime = new DateTime(descendant.getStat().getCtime());

                     listener.onZookeeperNodeAdded(
                         descendant.getPath(),
                         new String(descendant.getData()),
                         creationTime);
                 }
                 else if (eventType == TreeCacheEvent.Type.NODE_UPDATED) {
                     DateTime modificationTime = new DateTime(descendant.getStat().getMtime());

                     listener.onZookeeperNodeUpdated(
                         descendant.getPath(),
                         new String(descendant.getData()),
                         modificationTime);
                 }
                 else if (eventType == TreeCacheEvent.Type.NODE_REMOVED) {
                     listener.onZookeeperNodeDeleted(descendant.getPath());
                 }
            });

        if (subtreeCaches.containsKey(subtreeRootNodePath)) {
            ChildData subtreeRootNode = subtreeCache.getCurrentData(subtreeRootNodePath);

            if (subtreeRootNode != null) {
                listener.onZookeeperNodeAdded(
                    subtreeRootNodePath,
                    new String(subtreeRootNode.getData()),
                    new DateTime(subtreeRootNode.getStat().getCtime()));
            }

            Queue<String> subtreeTraverseQueue = new ArrayDeque<>();
            subtreeTraverseQueue.add(subtreeRootNodePath);

            while (!subtreeTraverseQueue.isEmpty()) {
                String currentSubtreeNodePath = subtreeTraverseQueue.remove();
                Map<String, ChildData> currentSubtreeNodeChildren = subtreeCache.getCurrentChildren(currentSubtreeNodePath);

                if (currentSubtreeNodeChildren != null) {
                    for (Map.Entry<String, ChildData> currentSubtreeNodeChild : currentSubtreeNodeChildren.entrySet()) {
                        String currentSubtreeNodeChildPath = makePath(currentSubtreeNodePath, currentSubtreeNodeChild.getKey());
                        String currentSubtreeNodeChildData = new String(currentSubtreeNodeChild.getValue().getData());
                        DateTime currentSubtreeNodeChildCreationTime = new DateTime(currentSubtreeNodeChild.getValue().getStat().getCtime());

                        listener.onZookeeperNodeAdded(
                            currentSubtreeNodeChildPath,
                            currentSubtreeNodeChildData,
                            currentSubtreeNodeChildCreationTime);

                        subtreeTraverseQueue.add(currentSubtreeNodeChildPath);
                    }
                }
            }
        }
        else {
            subtreeCaches.put(subtreeRootNodePath, subtreeCache);

            try {
                subtreeCache.start();
            }
            catch (Exception exception) {
                throw new ZookeeperException(exception);
            }
        }
    }

    private void closeSubtreeCaches() {
        for (TreeCache treeCache : subtreeCaches.values()) {
            try {
                treeCache.close();
            }
            catch (Exception exception) {
                logException(logger, exception);
            }
        }

        subtreeCaches.clear();
    }

    // ****************************** //

    @Override
    public synchronized List<String> getNodeChildNames(String path) {
        try {
            return client.getChildren().forPath(path);
        }
        catch (Exception exception) {
            throw new ZookeeperException(exception);
        }
    }

    @Override
    public synchronized void addNodeChildrenListener(String path, final ZookeeperNodeListener listener) {
        ensureNode(path);

        PathChildrenCache nodeChildrenCache =
            nodeChildrenCaches.containsKey(path) ?
            nodeChildrenCaches.get(path) :
            new PathChildrenCache(client, path, true);

        nodeChildrenCache.getListenable().addListener(
            (curatorFramework, event) -> {
                PathChildrenCacheEvent.Type eventType = event.getType();
                ChildData child = event.getData();

                if (eventType == CHILD_ADDED) {
                    DateTime creationTime = new DateTime(child.getStat().getCtime());

                    listener.onZookeeperNodeAdded(
                        child.getPath(),
                        new String(child.getData()),
                        creationTime);
                }
                else if (eventType == CHILD_UPDATED) {
                    DateTime modificationTime = new DateTime(child.getStat().getMtime());

                    listener.onZookeeperNodeUpdated(
                        child.getPath(),
                        new String(child.getData()),
                        modificationTime);
                }
                else if (eventType == CHILD_REMOVED) {
                    listener.onZookeeperNodeDeleted(child.getPath());
                }
            });

        if (!nodeChildrenCaches.containsKey(path)) {
            nodeChildrenCaches.put(path, nodeChildrenCache);

            try {
                nodeChildrenCache.start();
            }
            catch (Exception exception) {
                throw new ZookeeperException(exception);
            }
        }
        else {
            for (ChildData child : nodeChildrenCache.getCurrentData()) {
                DateTime creationTime = new DateTime(child.getStat().getCtime());

                listener.onZookeeperNodeAdded(
                    child.getPath(),
                    new String(child.getData()),
                    creationTime);
            }
        }
    }

    private void closeNodeChildrenCaches() {
        for (PathChildrenCache nodeChildrenCache : nodeChildrenCaches.values()) {
            try {
                nodeChildrenCache.close();
            }
            catch (Exception exception) {
                logException(logger, exception);
            }
        }

        nodeChildrenCaches.clear();
    }

    // ****************************** //

    private String connectionString;
    private CuratorFramework client;

    private final Map<String, TreeCache> subtreeCaches = new HashMap<>();
    private final Map<String, PathChildrenCache> nodeChildrenCaches = new HashMap<>();

    private static final Logger logger = getLogger(CuratorZookeeperConfig.class);
}
