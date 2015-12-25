package net.devromik.zookeeperConfig.curator;

import static net.devromik.zookeeperConfig.utils.net.LocalhostInfo.getLocalhost;

/**
 * @author Shulnyaev Roman
 */
public enum ZookeeperInstance {

    INSTANCE_1(1, 15001, 15002, 15003),
    INSTANCE_2(2, 16001, 16002, 16003),
    INSTANCE_3(3, 17001, 17002, 17003);

    // ****************************** //

    ZookeeperInstance(int serverId, int port, int electionPort, int quorumPort) {
        this.serverId = serverId;
        this.port = port;
        this.electionPort = electionPort;
        this.quorumPort = quorumPort;
    }

    public String getConnectionString() {
        return getLocalhost() + ":" + port;
    }

    // ****************************** //

    final int serverId;
    final int port;
    final int electionPort;
    final int quorumPort;
}
