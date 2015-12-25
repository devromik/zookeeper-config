package net.devromik.zookeeperConfig;

import java.util.List;
import org.junit.Test;
import static com.google.common.collect.Lists.newArrayList;
import static junit.framework.Assert.assertFalse;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

/**
 * @author Shulnyaev Roman
 */
public class ZookeeperTransactionBuilderTest {

    @Test
    public void buildsZookeeperTransaction() {
        ZookeeperTransaction transaction = new ZookeeperTransactionBuilder().
            createNode("/path_1", "data_1").
            createNode("/path_2", "data_2").
            delete("/path_3").
            createNode("/path_4", "data_4").
            delete("/path_5").
            delete("/path_6").
            createNode("/path_7", "data_7").
            build();

        assertThat(transaction.getOpCount(), is(7));
        assertFalse(transaction.isEmpty());

        List<ZookeeperOp> ops = newArrayList(transaction);

        CreateNodeOp createOp = (CreateNodeOp)ops.get(0);
        assertThat(createOp.getNodePath(), is("/path_1"));
        assertThat(createOp.getNodeData(), is("data_1"));

        createOp = (CreateNodeOp)ops.get(1);
        assertThat(createOp.getNodePath(), is("/path_2"));
        assertThat(createOp.getNodeData(), is("data_2"));

        DeleteNodeOp deleteOp = (DeleteNodeOp)ops.get(2);
        assertThat(deleteOp.getNodePath(), is("/path_3"));

        createOp = (CreateNodeOp)ops.get(3);
        assertThat(createOp.getNodePath(), is("/path_4"));
        assertThat(createOp.getNodeData(), is("data_4"));

        deleteOp = (DeleteNodeOp)ops.get(4);
        assertThat(deleteOp.getNodePath(), is("/path_5"));

        deleteOp = (DeleteNodeOp)ops.get(5);
        assertThat(deleteOp.getNodePath(), is("/path_6"));

        createOp = (CreateNodeOp)ops.get(6);
        assertThat(createOp.getNodePath(), is("/path_7"));
        assertThat(createOp.getNodeData(), is("data_7"));
    }

    @Test(expected = NullPointerException.class)
    public void createOpRequiresPath() {
        new ZookeeperTransactionBuilder().createNode(null, "data");
    }

    @Test(expected = NullPointerException.class)
    public void createOpRequiresData() {
        new ZookeeperTransactionBuilder().createNode("/path", null);
    }

    @Test(expected = NullPointerException.class)
    public void deleteOpRequiresPath() {
        new ZookeeperTransactionBuilder().delete(null);
    }
}
