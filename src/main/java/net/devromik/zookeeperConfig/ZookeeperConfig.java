package net.devromik.zookeeperConfig;

import java.util.List;
import static net.devromik.zookeeperConfig.utils.net.LocalhostInfo.getLocalhost;

/**
 * @author Shulnyaev Roman
 */
public interface ZookeeperConfig {

    String CONNECTION_STRING_JVM_PROPERTY_NAME = "zookeeper.connectionString";
    String DEFAULT_CONNECTION_STRING = getLocalhost() + ":" + 2181;

    // ****************************** //

    boolean nodeExists(String path);

    /**
     * Creates a persistent Zookeeper node with an empty data.
     * Causes any parent nodes to get created if they haven't already been.
     */
    void createNode(String path);

    /**
     * Creates a persistent Zookeeper node with the data {@code data}.
     * Causes any parent nodes to get created if they haven't already been.
     */
    void createNode(String path, String data);
    void ensureNode(String path);

    void updateNodeData(String path, String data);
    void deleteNode(String path);

    String getString(String path);
    int getInt(String path);

    void addSubtreeListener(String subtreeRootNodePath, ZookeeperNodeListener listener);

    List<String> getNodeChildNames(String path);
    void addNodeChildrenListener(String path, ZookeeperNodeListener listener);

    void executeTransaction(ZookeeperTransaction transaction);
}
