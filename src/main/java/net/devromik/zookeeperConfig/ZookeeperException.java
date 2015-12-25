package net.devromik.zookeeperConfig;

/**
 * @author Shulnyaev Roman
 */
public final class ZookeeperException extends RuntimeException {

    public ZookeeperException(String message) {
        super(message);
    }

    public ZookeeperException(Throwable cause) {
        super(cause);
    }
}
