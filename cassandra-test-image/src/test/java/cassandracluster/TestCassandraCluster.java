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

 import com.datastax.oss.driver.api.core.cql.ResultSet;
 import org.junit.Test;


 import static org.junit.Assert.assertTrue;

 public class TestCassandraCluster extends AbstractCassandraCluster
 {
     @Test
     public void testCassandraCluster()
     {
         mySession.execute(
                 "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', " +
                         "'datacenter1': 2}");
         mySession.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (id UUID PRIMARY KEY, value text)");
         ResultSet resultSet = mySession.execute("INSERT INTO test_keyspace.test_table (id, value) VALUES (uuid(), 'Test " +
                 "value')");

         assertTrue(resultSet.wasApplied());
     }
 }
