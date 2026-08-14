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
package com.ericsson.bss.cassandra.ecchronos.utils.enums.history;

/**
 * Represents the state of a repair session and defines valid state transitions.
 */
public enum SessionState
{
    /** The session has completed. */
    DONE(null),
    /** The session has started. Can transition to DONE. */
    STARTED(DONE),
    /** No state has been assigned yet. Can transition to STARTED. */
    NO_STATE(STARTED);

    private final SessionState nextValid;

    SessionState(final SessionState theNextValid)
    {
        this.nextValid = theNextValid;
    }

    /**
     * Checks whether this state can transition to the given next state.
     *
     * @param nextState the proposed next state.
     * @return {@code true} if the transition is valid, {@code false} otherwise.
     */
    public boolean canTransition(final SessionState nextState)
    {
        return nextState.equals(nextValid);
    }
}
