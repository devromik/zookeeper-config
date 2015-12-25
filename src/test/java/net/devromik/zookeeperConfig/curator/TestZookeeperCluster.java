package net.devromik.zookeeperConfig.curator;

import java.io.IOException;
import javax.annotation.*;
import org.junit.rules.TemporaryFolder;
import com.netflix.curator.test.*;

/**
 * @author Shulnyaev Roman
 */
public final class TestZookeeperCluster {

    @PostConstruct
    public void init() throws Exception {
        tempFolder.create();
        createCluster();
        cluster.start();
    }

    @PreDestroy
    public void destroy() throws Exception {
        try {
            cluster.close();
        }
        finally {
            tempFolder.delete();
        }
    }

    // ****************************** //

    public String getConnectionString() {
        return cluster.getConnectString();
    }

    // ****************************** //

    private void createCluster() throws IOException {
        InstanceSpec[] instances = new InstanceSpec[ZookeeperInstance.values().length];
        int i = 0;

        for (ZookeeperInstance instance : ZookeeperInstance.values()) {
            instances[i++] = new InstanceSpec(
                tempFolder.newFolder(),
                instance.port,
                instance.electionPort,
                instance.quorumPort,
                false,
                instance.serverId);
        }

        cluster = new TestingCluster(instances);
    }

    // ****************************** //

    private TestingCluster cluster;
    private TemporaryFolder tempFolder = new TemporaryFolder();
}
