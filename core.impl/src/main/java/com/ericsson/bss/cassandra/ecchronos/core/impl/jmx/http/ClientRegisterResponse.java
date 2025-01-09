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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Class used to construct Jolokia Client Register Response.
 */
public final class ClientRegisterResponse
{
    // CPD-OFF
    private Request myRequest;
    private Value myValue;
    private int myStatus;
    private long myTimestamp;

    public Request getRequest()
    {
        return myRequest;
    }

    public void setRequest(final Request request)
    {
        myRequest = request;
    }

    public Value getValue()
    {
        return myValue;
    }

    public void setValue(final Value value)
    {
        myValue = value;
    }

    public int getStatus()
    {
        return myStatus;
    }

    public void setStatus(final int status)
    {
        myStatus = status;
    }

    public long getTimestamp()
    {
        return myTimestamp;
    }

    public void setTimestamp(final long timestamp)
    {
        myTimestamp = timestamp;
    }

    @Override
    public String toString()
    {
        return "NotificationResponse{ request=" + myRequest
                + ", value=" + myValue + ", status=" + myStatus
                + ", timestamp=" + myTimestamp + '}';
    }

    public static final class Request
    {
        private String myType;
        private String myCommand;

        public String getType()
        {
            return myType;
        }

        public void setType(final String type)
        {
            myType = type;
        }

        public String getCommand()
        {
            return myCommand;
        }

        public void setCommand(final String command)
        {
            myCommand = command;
        }

        @Override
        public String toString()
        {
            return "Request{ type='" + myType + '\'' + ", command='" + myCommand + '\'' + '}';
        }
    }

    public static final class Value
    {
        private Backend myBackend;
        private String myId;

        public Backend getBackend()
        {
            return myBackend;
        }

        public void setBackend(final Backend backend)
        {
            myBackend = backend;
        }

        public String getId()
        {
            return myId;
        }

        public void setId(final String id)
        {
            myId = id;
        }

        @Override
        public String toString()
        {
            return "Value{ backend=" + myBackend
                    + ", id='" + myId + '\'' + '}';
        }
    }

    public static final class Backend
    {
        private Pull myPull;

        @JsonProperty("sse")
        private Map<String, String> mySse;

        public Pull getPull()
        {
            return myPull;
        }

        public void setPull(final Pull pull)
        {
            myPull = pull;
        }

        public Map<String, String> getSse()
        {
            return mySse;
        }

        public void setSse(final Map<String, String> sse)
        {
            mySse = sse;
        }

        @Override
        public String toString()
        {
            return "Backend{ pull=" + myPull + ", sse=" + mySse + '}';
        }
    }

    public static final class Pull
    {
        private int myMaxEntries;
        private String myStore;

        public int getMaxEntries()
        {
            return myMaxEntries;
        }

        public void setMaxEntries(final int maxEntries)
        {
            myMaxEntries = maxEntries;
        }

        public String getStore()
        {
            return myStore;
        }

        public void setStore(final String store)
        {
            myStore = store;
        }

        @Override
        public String toString()
        {
            return "Pull { maxEntries=" + myMaxEntries
                    + ", store='" + myStore + '\'' + '}';
        }
    }
}
