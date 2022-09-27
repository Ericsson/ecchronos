/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import java.util.Objects;

public class Credentials
{
    private boolean enabled;
    private String username;
    private String password;

    public Credentials()
    {
        // Default constructor
    }

    public Credentials(final boolean enabledValue, final String aUsername, final String aPassword)
    {
        this.enabled = enabledValue;
        this.username = aUsername;
        this.password = aPassword;
    }

    public final boolean isEnabled()
    {
        return enabled;
    }

    public final void setEnabled(final boolean enabledValue)
    {
        this.enabled = enabledValue;
    }

    public final String getUsername()
    {
        return username;
    }

    public final void setUsername(final String aUsername)
    {
        this.username = aUsername;
    }

    public final String getPassword()
    {
        return password;
    }

    public final void setPassword(final String aPassword)
    {
        this.password = aPassword;
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
        return enabled == that.enabled
                && username.equals(that.username)
                && password.equals(that.password);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(enabled, username, password);
    }
}
