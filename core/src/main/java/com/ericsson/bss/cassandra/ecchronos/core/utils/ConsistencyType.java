package com.ericsson.bss.cassandra.ecchronos.core.utils;

public enum ConsistencyType
{
    DEFAULT("DEFAULT"),
    LOCAL("LOCAL"),
    SERIAL("SERIAL");

    private final String consistencyTypeValue;

    ConsistencyType(final String consistentcyValue)
    {
        consistencyTypeValue = consistentcyValue;
    }

    public final String getStringValue()
    {
        return consistencyTypeValue;
    }
}
