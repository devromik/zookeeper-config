package net.devromik.zookeeperConfig;

import java.util.*;

/**
 * @author Shulnyaev Roman
 */
public final class ZookeeperTransaction implements Iterable<ZookeeperOp> {

    public void addOp(ZookeeperOp op) {
        ops.add(op);
    }

    @Override
    public Iterator<ZookeeperOp> iterator() {
        return ops.iterator();
    }

    public int getOpCount() {
        return ops.size();
    }

    public boolean isEmpty() {
        return ops.isEmpty();
    }

    // ****************************** //

    private final Collection<ZookeeperOp> ops = new ArrayList<>();
}
