package net.devromik.zookeeperConfig.curator;

import net.devromik.zookeeperConfig.CreateNodeOp;

/**
 * ;)
 *
 * @author Shulnyaev Roman
 */
public class NPETransactionOp extends CreateNodeOp {

    public NPETransactionOp(String path, String data) {
        super(path, data);
    }

    @Override
    public String getNodePath() {
        throw new NullPointerException();
    }
}
