package net.devromik.zookeeperConfig;

import org.joda.time.DateTime;

/**
 * @author Shulnyaev Roman
 */
public interface ZookeeperNodeListener {
    void onZookeeperNodeAdded(String path, String data, DateTime when);
    void onZookeeperNodeUpdated(String path, String data, DateTime when);
    void onZookeeperNodeDeleted(String path);
}
