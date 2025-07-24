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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class CRLConfig
{
    private static final int DEFAULT_ATTEMPTS = 5;
    private static final int DEFAULT_INTERVAL = 300;

    private boolean myEnabled = false;         // Defaults to false ("legacy"; e.g. no CRL checking)
    private String myPath = "";                // Defaults to no path
    private boolean myStrict = false;          // Defaults to non-strict mode
    private int myAttempts = DEFAULT_ATTEMPTS; // Defaults to five attempts before shutting down (in strict mode)
    private int myInterval = DEFAULT_INTERVAL; // Defaults to five minutes when looking for CRL file modifications

    public CRLConfig()
    {
    }

    @JsonProperty("enabled")
    public boolean getEnabled()
    {
        return myEnabled;
    }

    @JsonProperty("enabled")
    public void setEnabled(final boolean enabled)
    {
        this.myEnabled = enabled;
    }

    @JsonProperty("path")
    public String getPath()
    {
        return myPath;
    }

    @JsonProperty("path")
    public void setPath(final String path)
    {
        this.myPath = path;
    }

    @JsonProperty("strict")
    public boolean getStrict()
    {
        return myStrict;
    }

    @JsonProperty("strict")
    public void setStrict(final boolean strict)
    {
        this.myStrict = strict;
    }

    @JsonProperty("attempts")
    public int getAttempts()
    {
        return myAttempts;
    }

    @JsonProperty("attempts")
    public void setAttempts(final int attempts)
    {
        this.myAttempts = attempts;
    }

    @JsonProperty("interval")
    public int getInterval()
    {
        return myInterval;
    }

    @JsonProperty("interval")
    public void setInterval(final int interval)
    {
        this.myInterval = interval;
    }

}
