package net.devromik.zookeeperConfig.utils.net;

import java.net.UnknownHostException;
import static java.net.InetAddress.*;

/**
 * @author Shulnyaev Roman
 */
public final class LocalhostInfo {

    public static String getLocalhost() {
        try {
            return getLocalHost().getHostAddress();
        }
        catch (UnknownHostException exception) {
            return "localhost";
        }
    }
}
