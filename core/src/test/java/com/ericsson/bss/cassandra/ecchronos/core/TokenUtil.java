/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;

public class TokenUtil
{

    public static TokenRange getRange(long start, long end) throws Exception
    {
        Class<?> tokenFactoryClass = Class.forName("com.datastax.driver.core.Token$Factory");

        Constructor<TokenRange> tokenRangeConstructor = TokenRange.class.getDeclaredConstructor(Token.class, Token.class, tokenFactoryClass);

        tokenRangeConstructor.setAccessible(true);

        return tokenRangeConstructor.newInstance(new MockedToken(start), new MockedToken(end), null);
    }

    private static class MockedToken extends Token
    {
        private final long myValue;

        public MockedToken(long value)
        {
            myValue = value;
        }

        @Override
        public int compareTo(Token o)
        {
            if (o instanceof MockedToken)
            {
                return Long.compare(myValue, ((MockedToken) o).myValue);
            }
            else
            {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public DataType getType()
        {
            return DataType.bigint();
        }

        @Override
        public Object getValue()
        {
            return myValue;
        }

        @Override
        public ByteBuffer serialize(ProtocolVersion protocolVersion)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "Token{" + myValue + "}";
        }
    }
}
