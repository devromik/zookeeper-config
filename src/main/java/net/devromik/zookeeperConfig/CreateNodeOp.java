package net.devromik.zookeeperConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Shulnyaev Roman
 */
public class CreateNodeOp implements ZookeeperOp {

    public CreateNodeOp(String path, String data) {
        this.path = checkNotNull(path);
        this.data = checkNotNull(data);
    }

    @Override
    public String getNodePath() {
        return path;
    }

    public String getNodeData() {
        return data;
    }

    // ****************************** //

    private final String path;
    private final String data;
}