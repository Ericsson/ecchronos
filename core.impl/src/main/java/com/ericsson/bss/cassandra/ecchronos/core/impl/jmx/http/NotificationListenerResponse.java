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
     * Gets the value containing notification data.
     *
     * @return the value object.
     */
    public Value getValue()
    {
        return myValue;
    }

    /**
     * Sets the value containing notification data.
     *
     * @param value the value to set.
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

    /**
     * Represents the request portion of a Jolokia notification listener response.
     */
    public static final class Request
    {
        private String myMbean;
        private List<String> myArguments;
        private String myType;
        private String myOperation;

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
         * Gets the list of arguments for the request.
         *
         * @return the list of arguments.
         */
        public List<String> getArguments()
        {
            return myArguments;
        }

        /**
         * Sets the list of arguments for the request.
         *
         * @param arguments the arguments to set.
         */
        public void setArguments(final List<String> arguments)
        {
            myArguments = arguments;
        }

        /**
         * Gets the request type.
         *
         * @return the type.
         */
        public String getType()
        {
            return myType;
        }

        /**
         * Sets the request type.
         *
         * @param type the type to set.
         */
        public void setType(final String type)
        {
            myType = type;
        }

        /**
         * Gets the operation name.
         *
         * @return the operation name.
         */
        public String getOperation()
        {
            return myOperation;
        }

        /**
         * Sets the operation name.
         *
         * @param operation the operation name to set.
         */
        public void setOperation(final String operation)
        {
            myOperation = operation;
        }
    }

    /**
     * Represents the value portion of a Jolokia notification listener response,
     * containing notification data and metadata.
     */
    public static final class Value
    {
        private int myDropped;
        private String myHandle;
        private Object myHandback;
        private List<Notification> myNotifications;

        /**
         * Gets the number of dropped notifications.
         *
         * @return the number of dropped notifications.
         */
        public int getDropped()
        {
            return myDropped;
        }

        /**
         * Sets the number of dropped notifications.
         *
         * @param dropped the number of dropped notifications to set.
         */
        public void setDropped(final int dropped)
        {
            myDropped = dropped;
        }

        /**
         * Gets the notification listener handle.
         *
         * @return the handle string.
         */
        public String getHandle()
        {
            return myHandle;
        }

        /**
         * Sets the notification listener handle.
         *
         * @param handle the handle string to set.
         */
        public void setHandle(final String handle)
        {
            myHandle = handle;
        }

        /**
         * Gets the handback object associated with the listener.
         *
         * @return the handback object.
         */
        public Object getHandback()
        {
            return myHandback;
        }

        /**
         * Sets the handback object associated with the listener.
         *
         * @param handback the handback object to set.
         */
        public void setHandback(final Object handback)
        {
            myHandback = handback;
        }

        /**
         * Gets the list of notifications.
         *
         * @return the list of notifications.
         */
        public List<Notification> getNotifications()
        {
            return myNotifications;
        }

        /**
         * Sets the list of notifications.
         *
         * @param notifications the notifications to set.
         */
        public void setNotifications(final List<Notification> notifications)
        {
            myNotifications = notifications;
        }
    }

    /**
     * Represents an individual JMX notification within the response.
     */
    public static final class Notification
    {
        private long myTimeStamp;
        private long mySequenceNumber;
        private Map<String, Object> myUserData;
        private String mySource;
        private String myMessage;
        private String myType;

        /**
         * Gets the timestamp of the notification.
         *
         * @return the timestamp.
         */
        public long getTimeStamp()
        {
            return myTimeStamp;
        }

        /**
         * Sets the timestamp of the notification.
         *
         * @param timeStamp the timestamp to set.
         */
        public void setTimeStamp(final long timeStamp)
        {
            myTimeStamp = timeStamp;
        }

        /**
         * Gets the sequence number of the notification.
         *
         * @return the sequence number.
         */
        public long getSequenceNumber()
        {
            return mySequenceNumber;
        }

        /**
         * Sets the sequence number of the notification.
         *
         * @param sequenceNumber the sequence number to set.
         */
        public void setSequenceNumber(final long sequenceNumber)
        {
            mySequenceNumber = sequenceNumber;
        }

        /**
         * Gets the user data associated with the notification.
         *
         * @return the user data map.
         */
        public Map<String, Object> getUserData()
        {
            return myUserData;
        }

        /**
         * Sets the user data associated with the notification.
         *
         * @param userData the user data map to set.
         */
        public void setUserData(final Map<String, Object> userData)
        {
            myUserData = userData;
        }

        /**
         * Gets the source of the notification.
         *
         * @return the source string.
         */
        public String getSource()
        {
            return mySource;
        }

        /**
         * Sets the source of the notification.
         *
         * @param source the source string to set.
         */
        public void setSource(final String source)
        {
            mySource = source;
        }

        /**
         * Gets the notification message.
         *
         * @return the message string.
         */
        public String getMessage()
        {
            return myMessage;
        }

        /**
         * Sets the notification message.
         *
         * @param message the message string to set.
         */
        public void setMessage(final String message)
        {
            myMessage = message;
        }

        /**
         * Gets the type of the notification.
         *
         * @return the type string.
         */
        public String getType()
        {
            return myType;
        }

        /**
         * Sets the type of the notification.
         *
         * @param type the type string to set.
         */
        public void setType(final String type)
        {
            myType = type;
        }
    }
}
