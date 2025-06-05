package cassandracluster;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.base.Preconditions;

import java.util.Collection;

public class MockNodeWorkManager extends NodeWorkerManager{

    Integer newNodesCount = 0;



    public MockNodeWorkManager(final NodeWorkerManager.Builder builder)
    {
        super(builder);
    }
    @Override
    protected void addNewNodeToThreadPool(final Node node)
    {
        newNodesCount++;
        NodeWorker worker = new MockNodeWorker(
                node,
                getMyReplicatedTableProvider(),
                getMyRepairScheduler(),
                Preconditions.checkNotNull(getMyTableReferenceFactory(),
                        "Table reference factory must be set"),
                getMyRepairConfigurationFunction(),
                getMyNativeConnectionProvider().getCqlSession());
        getMyWorkers().put(node, worker);
        getMyThreadPool().submit(worker);
    }
}
