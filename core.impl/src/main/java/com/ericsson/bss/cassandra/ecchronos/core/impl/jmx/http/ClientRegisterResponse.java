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
    /**
     * Default constructor.
     */
    public ClientRegisterResponse()
    {
        // Default constructor
    }

    private Request myRequest;
    private Value myValue;
    private int myStatus;
    private long myTimestamp;

    /**
     * Gets the request details of this response.
     *
     * @return the request object.
     */
    public Request getRequest()
    {
        return myRequest;
    }

    /**
     * Sets the request details of this response.
     *
     * @param request the request object to set.
     */
    public void setRequest(final Request request)
    {
        myRequest = request;
    }

    /**
     * Gets the value containing backend and registration details.
     *
     * @return the value object.
     */
    public Value getValue()
    {
        return myValue;
    }

    /**
     * Sets the value containing backend and registration details.
     *
     * @param value the value object to set.
     */
    public void setValue(final Value value)
    {
        myValue = value;
    }

    /**
     * Gets the HTTP status code of the response.
     *
     * @return the status code.
     */
    public int getStatus()
    {
        return myStatus;
    }

    /**
     * Sets the HTTP status code of the response.
     *
     * @param status the status code to set.
     */
    public void setStatus(final int status)
    {
        myStatus = status;
    }

    /**
     * Gets the timestamp of the response.
     *
     * @return the timestamp in epoch seconds.
     */
    public long getTimestamp()
    {
        return myTimestamp;
    }

    /**
     * Sets the timestamp of the response.
     *
     * @param timestamp the timestamp to set in epoch seconds.
     */
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

    /**
     * Represents the request part of a Jolokia client register response.
     */
    public static final class Request
    {
        /**
         * Default constructor.
         */
        public Request()
        {
            // Default constructor
        }

        private String myType;
        private String myCommand;

        /**
         * Gets the request type.
         *
         * @return the type string.
         */
        public String getType()
        {
            return myType;
        }

        /**
         * Sets the request type.
         *
         * @param type the type string to set.
         */
        public void setType(final String type)
        {
            myType = type;
        }

        /**
         * Gets the request command.
         *
         * @return the command string.
         */
        public String getCommand()
        {
            return myCommand;
        }

        /**
         * Sets the request command.
         *
         * @param command the command string to set.
         */
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

    /**
     * Represents the value part of a Jolokia client register response, containing backend and ID information.
     */
    public static final class Value
    {
        /**
         * Default constructor.
         */
        public Value()
        {
            // Default constructor
        }

        private Backend myBackend;
        private String myId;

        /**
         * Gets the backend configuration.
         *
         * @return the backend object.
         */
        public Backend getBackend()
        {
            return myBackend;
        }

        /**
         * Sets the backend configuration.
         *
         * @param backend the backend object to set.
         */
        public void setBackend(final Backend backend)
        {
            myBackend = backend;
        }

        /**
         * Gets the client registration ID.
         *
         * @return the registration ID string.
         */
        public String getId()
        {
            return myId;
        }

        /**
         * Sets the client registration ID.
         *
         * @param id the registration ID string to set.
         */
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

    /**
     * Represents the backend configuration of a Jolokia notification registration.
     */
    public static final class Backend
    {
        /**
         * Default constructor.
         */
        public Backend()
        {
            // Default constructor
        }

        private Pull myPull;

        @JsonProperty("sse")
        private Map<String, String> mySse;

        /**
         * Gets the pull-based notification configuration.
         *
         * @return the pull configuration.
         */
        public Pull getPull()
        {
            return myPull;
        }

        /**
         * Sets the pull-based notification configuration.
         *
         * @param pull the pull configuration to set.
         */
        public void setPull(final Pull pull)
        {
            myPull = pull;
        }

        /**
         * Gets the SSE (Server-Sent Events) configuration map.
         *
         * @return the SSE configuration map.
         */
        public Map<String, String> getSse()
        {
            return mySse;
        }

        /**
         * Sets the SSE (Server-Sent Events) configuration map.
         *
         * @param sse the SSE configuration map to set.
         */
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

    /**
     * Represents the pull-based notification backend configuration.
     */
    public static final class Pull
    {
        /**
         * Default constructor.
         */
        public Pull()
        {
            // Default constructor
        }

        private int myMaxEntries;
        private String myStore;

        /**
         * Gets the maximum number of notification entries that can be stored.
         *
         * @return the maximum entries count.
         */
        public int getMaxEntries()
        {
            return myMaxEntries;
        }

        /**
         * Sets the maximum number of notification entries that can be stored.
         *
         * @param maxEntries the maximum entries count to set.
         */
        public void setMaxEntries(final int maxEntries)
        {
            myMaxEntries = maxEntries;
        }

        /**
         * Gets the notification store type.
         *
         * @return the store type string.
         */
        public String getStore()
        {
            return myStore;
        }

        /**
         * Sets the notification store type.
         *
         * @param store the store type string to set.
         */
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
