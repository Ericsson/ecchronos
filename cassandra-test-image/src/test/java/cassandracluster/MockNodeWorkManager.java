/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
