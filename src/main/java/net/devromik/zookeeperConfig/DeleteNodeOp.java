package net.devromik.zookeeperConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Shulnyaev Roman
 */
public final class DeleteNodeOp implements ZookeeperOp {

    public DeleteNodeOp(String path) {
        this.path = checkNotNull(path);
    }

    @Override
    public String getNodePath() {
        return path;
    }

    // ****************************** //

    private final String path;
}