/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;

/**
 * A factory creating JMX proxies to Cassandra.
 */
@SuppressWarnings("FinalClass")
public class JmxProxyFactoryImpl implements JmxProxyFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(JmxProxyFactoryImpl.class);

    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    private static final String RS_OBJ_NAME = "org.apache.cassandra.db:type=RepairService";

    private static final String REPAIR_ASYNC_METHOD = "repairAsync";
    private static final String REPAIR_STATS_METHOD = "getRepairStats";
    private static final String FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD = "forceTerminateAllRepairSessions";
    private static final String LIVE_NODES_ATTRIBUTE = "LiveNodes";
    private static final String UNREACHABLE_NODES_ATTRIBUTE = "UnreachableNodes";

    private final JmxConnectionProvider myJmxConnectionProvider;

    private JmxProxyFactoryImpl(final Builder builder)
    {
        myJmxConnectionProvider = builder.myJmxConnectionProvider;
    }

    @Override
    public JmxProxy connect() throws IOException
    {
        try
        {
            JMXConnector jmxConnector = myJmxConnectionProvider.getJmxConnector();

            return new InternalJmxProxy(jmxConnector,
                    jmxConnector.getMBeanServerConnection(),
                    new ObjectName(SS_OBJ_NAME), new ObjectName(RS_OBJ_NAME));
        }
        catch (MalformedObjectNameException e)
        {
            throw new IOException("Unable to get StorageService object", e);
        }
    }

    private final class InternalJmxProxy implements JmxProxy
    {
        private final JMXConnector myJmxConnector;
        private final MBeanServerConnection myMbeanServerConnection;
        private final ObjectName myStorageServiceObject;
        private final ObjectName myRepairServiceObject;

        private InternalJmxProxy(final JMXConnector jmxConnector,
                                 final MBeanServerConnection mbeanServerConnection,
                                 final ObjectName storageServiceObject,
                                 final ObjectName repairServiceObject)
        {
            myJmxConnector = jmxConnector;
            myMbeanServerConnection = mbeanServerConnection;
            myStorageServiceObject = storageServiceObject;
            myRepairServiceObject = repairServiceObject;
        }

        @Override
        public void close()
        {
            // Should not close
        }

        @Override
        public void addStorageServiceListener(final NotificationListener listener)
        {
            try
            {
                myJmxConnector.addConnectionNotificationListener(listener, null, null);
                myMbeanServerConnection.addNotificationListener(myStorageServiceObject, listener, null, null);
            }
            catch (InstanceNotFoundException | IOException e)
            {
                LOG.error("Unable to add StorageService listener", e);
            }
        }

        @SuppressWarnings ("unchecked")
        @Override
        public List<String> getLiveNodes()
        {
            try
            {
                return (List<String>) myMbeanServerConnection.getAttribute(myStorageServiceObject,
                        LIVE_NODES_ATTRIBUTE);
            }
            catch (InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | AttributeNotFoundException e)
            {
                LOG.error("Unable to get live nodes", e);
            }
            return Collections.emptyList();
        }

        @SuppressWarnings ("unchecked")
        @Override
        public List<String> getUnreachableNodes()
        {
            try
            {
                return (List<String>) myMbeanServerConnection.getAttribute(myStorageServiceObject,
                        UNREACHABLE_NODES_ATTRIBUTE);
            }
            catch (InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | AttributeNotFoundException e)
            {
                LOG.error("Unable to get unreachable nodes", e);
            }
            return Collections.emptyList();
        }

        @Override
        public int repairAsync(final String keyspace, final Map<String, String> options)
        {
            try
            {
                return (int) myMbeanServerConnection.invoke(myStorageServiceObject,
                        REPAIR_ASYNC_METHOD,
                        new Object[]
                        {
                                keyspace, options
                        },
                        new String[]
                        {
                                String.class.getName(), Map.class.getName()
                        });
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
            {
                LOG.error("Unable to repair", e);
            }

            return 0;
        }

        /**
         * Force terminate all repair sessions.
         */
        @Override
        public void forceTerminateAllRepairSessions()
        {
            try
            {
                myMbeanServerConnection.invoke(myStorageServiceObject,
                        FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD,
                        null, null);
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
            {
                LOG.error("Unable to terminate repair sessions");
            }
        }

        /**
         * Remove the storage service listener.
         *
         * @param listener
         *            The listener to remove.
         *
         */
        @Override
        public void removeStorageServiceListener(final NotificationListener listener)
        {
            try
            {
                myJmxConnector.removeConnectionNotificationListener(listener);
                myMbeanServerConnection.removeNotificationListener(myStorageServiceObject, listener);
            }
            catch (InstanceNotFoundException | ListenerNotFoundException | IOException e)
            {
                LOG.error("Unable to remove StorageService listener", e);
            }
        }

        /**
         * Get the live disk space used.
         *
         * @param tableReference
         *            The table to get the live disk space for.
         * @return long
         */
        @Override
        public long liveDiskSpaceUsed(final TableReference tableReference)
        {
            try
            {
                ObjectName objectName
                        = new ObjectName(String
                        .format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=LiveDiskSpaceUsed",
                                tableReference.getKeyspace(), tableReference.getTable()));

                return (Long) myMbeanServerConnection.getAttribute(objectName, "Count");
            }
            catch (AttributeNotFoundException
                   | InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | MalformedObjectNameException e)
            {
                LOG.error("Unable to retrieve disk space usage for {}", tableReference, e);
            }

            return 0;
        }

        @Override
        public long getMaxRepairedAt(final TableReference tableReference)
        {
            try
            {
                List<String> args = new ArrayList<>();
                args.add(tableReference.getKeyspace());
                args.add(tableReference.getTable());
                List<CompositeData> compositeDatas = (List<CompositeData>) myMbeanServerConnection.invoke(
                        myRepairServiceObject, REPAIR_STATS_METHOD,
                        new Object[]
                        {
                                args, null
                        },
                        new String[]
                        {
                                List.class.getName(),
                                String.class.getName()
                        });
                for (CompositeData data : compositeDatas)
                {
                    long maxRepaired = (long) data.getAll(new String[]{"maxRepaired"})[0];
                    return maxRepaired;
                }
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
            {
                LOG.error("Unable to get maxRepaired for {}", tableReference, e);
            }
            return 0;
        }

        @Override
        public double getPercentRepaired(final TableReference tableReference)
        {
            try
            {
                ObjectName objectName
                        = new ObjectName(String
                        .format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=PercentRepaired",
                                tableReference.getKeyspace(), tableReference.getTable()));

                double percentRepaired = (double) myMbeanServerConnection.getAttribute(objectName, "Value");
                return percentRepaired;
            }
            catch (AttributeNotFoundException
                   | InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | MalformedObjectNameException e)
            {
                LOG.error("Unable to retrieve disk space usage for {}", tableReference, e);
            }
            return 0.0;
        }

        @Override
        public String getNodeStatus()
        {
            try
            {
                return (String) myMbeanServerConnection.getAttribute(myStorageServiceObject, "OperationMode");
            }
            catch (Exception e)
            {
                LOG.error("Unable to retrieve node status {}", e.getMessage());
                return "Unknown";
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private JmxConnectionProvider myJmxConnectionProvider;

        /**
         * Build with JMX connection provider.
         *
         * @param jmxConnectionProvider The JMX connection provider
         * @return Builder
         */
        public Builder withJmxConnectionProvider(final JmxConnectionProvider jmxConnectionProvider)
        {
            myJmxConnectionProvider = jmxConnectionProvider;
            return this;
        }

        /**
         * Build.
         *
         * @return JmxProxyFactoryImpl
         */
        public JmxProxyFactoryImpl build()
        {
            if (myJmxConnectionProvider == null)
            {
                throw new IllegalArgumentException("JMX Connection provider cannot be null");
            }

            return new JmxProxyFactoryImpl(this);
        }
    }
}
