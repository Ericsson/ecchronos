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
package com.ericsson.bss.cassandra.ecchronos.application.spring;


import com.datastax.oss.driver.api.core.metadata.Node;

public class NodeChangeRecord
{
    enum NodeChangeType
    {
        INSERT,
        DELETE,
        UPDATE
    }

    private final Node myNode;
    private final NodeChangeType myType;

    public NodeChangeRecord(final Node node, final NodeChangeType type)
    {
        this.myNode = node;
        this.myType = type;
    }

    /***
     * returns the node that has changed.
     *
     * @return
     */
    public  Node getNode()
    {
        return myNode;
    }

    /***
     * Returns the change type can be either INSERT, DELETE or UPDATE.
     * @return
     */
    public NodeChangeType getType()
    {
        return myType;
    }
}
