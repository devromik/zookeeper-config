package net.devromik.zookeeperConfig.curator;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.*;

/**
 * @author Shulnyaev Roman (Roman.Shulnyaev@billing.ru)
 */
public final class NodePathAndData {

    public NodePathAndData(String path, String data) {
        this.path = checkNotNull(path);
        this.data = checkNotNull(data);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other.getClass() != getClass()) {
            return false;
        }

        NodePathAndData otherNodePathAndData = (NodePathAndData)other;
        return path.equals(otherNodePathAndData.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return toStringHelper(this).
            add("path", path).
            add("data", data).toString();
    }

    // ****************************** //

    private final String path;
    private final String data;
}
