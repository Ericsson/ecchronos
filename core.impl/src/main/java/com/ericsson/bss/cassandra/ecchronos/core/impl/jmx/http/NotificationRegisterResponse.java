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

    public Request getRequest()
    {
        return myRequest;
    }

    public void setRequest(final Request request)
    {
        myRequest = request;
    }

    public String getValue()
    {
        return myValue;
    }

    public void setValue(final String value)
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
        return "NotificationAddResponse{ request=" + myRequest
                + ", value='" + myValue
                + '\'' + ", status="
                + myStatus + ", timestamp="
                + myTimestamp + '}';
    }

    public static final class Request
    {
        private String myMode;
        private List<String> myFilter;
        private String myMbean;
        private String myClient;
        private String myType;
        private String myCommand;

        public String getMode()
        {
            return myMode;
        }

        public void setMode(final String mode)
        {
            myMode = mode;
        }

        public List<String> getFilter()
        {
            return myFilter;
        }

        public void setFilter(final List<String> filter)
        {
            myFilter = filter;
        }

        public String getMbean()
        {
            return myMbean;
        }

        public void setMbean(final String mbean)
        {
            myMbean = mbean;
        }

        public String getClient()
        {
            return myClient;
        }

        public void setClient(final String client)
        {
            myClient = client;
        }

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
