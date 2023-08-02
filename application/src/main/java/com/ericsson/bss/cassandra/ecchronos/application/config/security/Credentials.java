/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import java.util.Objects;

public class Credentials
{
    private boolean myIsEnabled;
    private String myUsername;
    private String myPassword;

    public Credentials()
    {
        // Default constructor
    }

    public Credentials(final boolean enabled, final String username, final String password)
    {
        myIsEnabled = enabled;
        myUsername = username;
        myPassword = password;
    }

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    @JsonProperty("username")
    public final String getUsername()
    {
        return myUsername;
    }

    @JsonProperty("username")
    public final void setUsername(final String username)
    {
        myUsername = username;
    }

    @JsonProperty("password")
    public final String getPassword()
    {
        return myPassword;
    }

    @JsonProperty("password")
    public final void setPassword(final String password)
    {
        myPassword = password;
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        Credentials that = (Credentials) o;
        return myIsEnabled == that.myIsEnabled
                && myUsername.equals(that.myUsername)
                && myPassword.equals(that.myPassword);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(myIsEnabled, myUsername, myPassword);
    }
}
