package net.devromik.zookeeperConfig;

/**
 * @author Shulnyaev Roman
 */
public final class ZookeeperTransactionBuilder {

    public ZookeeperTransactionBuilder createNode(String path, String data) {
        this.transaction.addOp(new CreateNodeOp(path, data));
        return this;
    }

    public ZookeeperTransactionBuilder delete(String path) {
        this.transaction.addOp(new DeleteNodeOp(path));
        return this;
    }

    public ZookeeperTransaction build() {
        return transaction;
    }

    // ****************************** //

    private final ZookeeperTransaction transaction = new ZookeeperTransaction();
}
