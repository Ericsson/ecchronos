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
import java.util.Map;

/**
 * Class used to construct Jolokia Notification Listener Response.
 */
public final class NotificationListenerResponse
{
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

    public static final class Request
    {
        private String myMbean;
        private List<String> myArguments;
        private String myType;
        private String myOperation;

        public String getMbean()
        {
            return myMbean;
        }

        public void setMbean(final String mbean)
        {
            myMbean = mbean;
        }

        public List<String> getArguments()
        {
            return myArguments;
        }

        public void setArguments(final List<String> arguments)
        {
            myArguments = arguments;
        }

        public String getType()
        {
            return myType;
        }

        public void setType(final String type)
        {
            myType = type;
        }

        public String getOperation()
        {
            return myOperation;
        }

        public void setOperation(final String operation)
        {
            myOperation = operation;
        }
    }

    public static final class Value
    {
        private int myDropped;
        private String myHandle;
        private Object myHandback;
        private List<Notification> myNotifications;

        public int getDropped()
        {
            return myDropped;
        }

        public void setDropped(final int dropped)
        {
            myDropped = dropped;
        }

        public String getHandle()
        {
            return myHandle;
        }

        public void setHandle(final String handle)
        {
            myHandle = handle;
        }

        public Object getHandback()
        {
            return myHandback;
        }

        public void setHandback(final Object handback)
        {
            myHandback = handback;
        }

        public List<Notification> getNotifications()
        {
            return myNotifications;
        }

        public void setNotifications(final List<Notification> notifications)
        {
            myNotifications = notifications;
        }
    }

    public static final class Notification
    {
        private long myTimeStamp;
        private long mySequenceNumber;
        private Map<String, Object> myUserData;
        private String mySource;
        private String myMessage;
        private String myType;

        public long getTimeStamp()
        {
            return myTimeStamp;
        }

        public void setTimeStamp(final long timeStamp)
        {
            myTimeStamp = timeStamp;
        }

        public long getSequenceNumber()
        {
            return mySequenceNumber;
        }

        public void setSequenceNumber(final long sequenceNumber)
        {
            mySequenceNumber = sequenceNumber;
        }

        public Map<String, Object> getUserData()
        {
            return myUserData;
        }

        public void setUserData(final Map<String, Object> userData)
        {
            myUserData = userData;
        }

        public String getSource()
        {
            return mySource;
        }

        public void setSource(final String source)
        {
            mySource = source;
        }

        public String getMessage()
        {
            return myMessage;
        }

        public void setMessage(final String message)
        {
            myMessage = message;
        }

        public String getType()
        {
            return myType;
        }

        public void setType(final String type)
        {
            myType = type;
        }
    }
}
