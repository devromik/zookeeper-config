package net.devromik.zookeeperConfig.curator;

import java.util.*;
import java.util.concurrent.*;
import org.joda.time.DateTime;
import net.devromik.zookeeperConfig.ZookeeperNodeListener;

/**
 * @author Shulnyaev Roman
 */
public final class TestZookeeperNodeListener implements ZookeeperNodeListener {

    @Override
    public void onZookeeperNodeAdded(String path, String data, DateTime when) {
        addedNodes.add(new NodePathAndData(path, data));
    }

    public Set<NodePathAndData> getAddedNodes() {
        return addedNodes;
    }

    // ****************************** //

    @Override
    public void onZookeeperNodeUpdated(String path, String data, DateTime when) {
        updatedNodes.add(new NodePathAndData(path, data));
    }

    public Set<NodePathAndData> getUpdatedNodes() {
        return updatedNodes;
    }

    // ****************************** //

    @Override
    public void onZookeeperNodeDeleted(String path) {
        deletedNodePaths.add(path);
    }

    public Set<String> getDeletedNodePaths() {
        return deletedNodePaths;
    }

    // ****************************** //

    private final Set<NodePathAndData> addedNodes = new CopyOnWriteArraySet<>();
    private final Set<NodePathAndData> updatedNodes = new CopyOnWriteArraySet<>();
    private final Set<String> deletedNodePaths = new CopyOnWriteArraySet<>();
}
