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

import java.util.List;

/**
 * Class used to construct Jolokia Notification Register Response.
 */
public final class NotificationRegisterResponse
{
    private Request myRequest;
    private String myValue;
    private int myStatus;
    private long myTimestamp;

    /**
     * Gets the request associated with this response.
     *
     * @return the request object.
     */
    public Request getRequest()
    {
        return myRequest;
    }

    /**
     * Sets the request associated with this response.
     *
     * @param request the request to set.
     */
    public void setRequest(final Request request)
    {
        myRequest = request;
    }

    /**
     * Gets the value of this response (the notification handle).
     *
     * @return the value string.
     */
    public String getValue()
    {
        return myValue;
    }

    /**
     * Sets the value of this response.
     *
     * @param value the value string to set.
     */
    public void setValue(final String value)
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
     * @return the timestamp in milliseconds.
     */
    public long getTimestamp()
    {
        return myTimestamp;
    }

    /**
     * Sets the timestamp of the response.
     *
     * @param timestamp the timestamp to set in milliseconds.
     */
    public void setTimestamp(final long timestamp)
    {
        myTimestamp = timestamp;
    }

    @Override
    public String toString()
    {
        return "NotificationAddResponse{ request=" + myRequest
                + ", value='" + myValue
                + '\'' + ", status="
                + myStatus + ", timestamp="
                + myTimestamp + '}';
    }

    /**
     * Represents the request portion of a Jolokia notification register response.
     */
    public static final class Request
    {
        private String myMode;
        private List<String> myFilter;
        private String myMbean;
        private String myClient;
        private String myType;
        private String myCommand;

        /**
         * Gets the notification mode.
         *
         * @return the mode string.
         */
        public String getMode()
        {
            return myMode;
        }

        /**
         * Sets the notification mode.
         *
         * @param mode the mode string to set.
         */
        public void setMode(final String mode)
        {
            myMode = mode;
        }

        /**
         * Gets the notification filter list.
         *
         * @return the list of filter strings.
         */
        public List<String> getFilter()
        {
            return myFilter;
        }

        /**
         * Sets the notification filter list.
         *
         * @param filter the list of filter strings to set.
         */
        public void setFilter(final List<String> filter)
        {
            myFilter = filter;
        }

        /**
         * Gets the MBean name.
         *
         * @return the MBean name.
         */
        public String getMbean()
        {
            return myMbean;
        }

        /**
         * Sets the MBean name.
         *
         * @param mbean the MBean name to set.
         */
        public void setMbean(final String mbean)
        {
            myMbean = mbean;
        }

        /**
         * Gets the client identifier.
         *
         * @return the client identifier.
         */
        public String getClient()
        {
            return myClient;
        }

        /**
         * Sets the client identifier.
         *
         * @param client the client identifier to set.
         */
        public void setClient(final String client)
        {
            myClient = client;
        }

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
         * Gets the command name.
         *
         * @return the command string.
         */
        public String getCommand()
        {
            return myCommand;
        }

        /**
         * Sets the command name.
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
            return "Request{ mode='"
                    + myMode + '\''
                    + ", filter="
                    + myFilter
                    + ", mbean='"
                    + myMbean + '\''
                    + ", client='" + myClient + '\''
                    + ", type='" + myType + '\''
                    + ", command='" + myCommand + '\'' + '}';
        }
    }
}
