package cassandracluster;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.Set;
import java.util.function.Function;

public class MockNodeWorker extends NodeWorker {

    Integer tableCreateCount = 0;
    Integer tableRemoveCount = 0;
    Integer keyspaceCreateCount = 0;


    public MockNodeWorker(
            final Node node,
            final ReplicatedTableProvider replicatedTableProvider,
            final RepairScheduler repairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final Function<TableReference, Set<RepairConfiguration>> repairConfigurationFunction,
            final CqlSession session)
    {
        super (node,replicatedTableProvider, repairScheduler, tableReferenceFactory, repairConfigurationFunction, session);
    }
    @Override
    protected void onTableCreated(final TableCreatedEvent tableEvent)
    {
        tableCreateCount ++;
        super.onTableCreated(tableEvent);


    }
    @Override
    protected void removeConfiguration(final TableMetadata table){
        tableRemoveCount++;
        super.removeConfiguration(table);
    }

    @Override
    protected void onKeyspaceCreated(final KeyspaceCreatedEvent keyspaceEvent)
    {
        keyspaceCreateCount++;
        super.onKeyspaceCreated(keyspaceEvent);
    }
    public Integer getTableCreateCount() {
        return tableCreateCount;
    }
    public Integer getTableRemoveCount() {
        return tableRemoveCount;
    }

    public Integer getKeyspaceCreateCount() {
        return keyspaceCreateCount;
    }

}
