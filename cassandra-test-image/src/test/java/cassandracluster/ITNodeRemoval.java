/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

public class ITNodeRemoval extends AbstractCassandraCluster
{
    private static final Logger LOG = LoggerFactory.getLogger(ITNodeRemoval.class);

    @Test
    public void testNodeDecommissionedFromCluster() throws InterruptedException
    {
        DefaultRepairConfigurationProvider listener = mock(DefaultRepairConfigurationProvider.class);

        mySession = defaultBuilder().withNodeStateListener(listener).build();

        try
        {
            decommissionNode("cassandra-seed-dc1-rack1-node1");
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        LOG.info("Waiting for node to be decommissioned.");
        waitForNodesToBeUp("cassandra-seed-dc2-rack1-node1",4,DEFAULT_WAIT_TIME_IN_MS);

        verify(listener, times(1)).onRemove(any());
    }
}
