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
package com.ericsson.bss.cassandra.ecchronos.application.config.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Aggregates all metrics reporting configurations. */
public class ReportingConfigs
{
    private ReportingConfig myJmxReportingConfig = new ReportingConfig();
    private ReportingConfig myFileReportingConfig = new ReportingConfig();
    private ReportingConfig myHttpReportingConfig = new ReportingConfig();

    /** Default constructor. */
    public ReportingConfigs()
    {
    }

    /**
     * Returns the JMX reporting config.
     * @return the JMX reporting config
     */
    @JsonProperty("jmx")
    public final ReportingConfig getJmxReportingConfig()
    {
        return myJmxReportingConfig;
    }

    /**
     * Returns the file reporting config.
     * @return the file reporting config
     */
    @JsonProperty("file")
    public final ReportingConfig getFileReportingConfig()
    {
        return myFileReportingConfig;
    }

    /**
     * Returns the HTTP reporting config.
     * @return the HTTP reporting config
     */
    @JsonProperty("http")
    public final ReportingConfig getHttpReportingConfig()
    {
        return myHttpReportingConfig;
    }

    /**
     * Sets the JMX reporting config.
     * @param jmxReportingConfig the JMX reporting config
     */
    @JsonProperty("jmx")
    public final void setJmxReportingConfig(final ReportingConfig jmxReportingConfig)
    {
        myJmxReportingConfig = jmxReportingConfig;
    }

    /**
     * Sets the file reporting config.
     * @param fileReportingConfig the file reporting config
     */
    @JsonProperty("file")
    public final void setFileReportingConfig(final ReportingConfig fileReportingConfig)
    {
        myFileReportingConfig = fileReportingConfig;
    }

    /**
     * Sets the HTTP reporting config.
     * @param httpReportingConfig the HTTP reporting config
     */
    @JsonProperty("http")
    public final void setHttpReportingConfig(final ReportingConfig httpReportingConfig)
    {
        myHttpReportingConfig = httpReportingConfig;
    }

    /**
     * Returns whether HTTP reporting enabled.
     * @return true if HTTP reporting enabled
     */
    public final boolean isHttpReportingEnabled()
    {
        return myHttpReportingConfig != null && myHttpReportingConfig.isEnabled();
    }

    /**
     * Returns whether JMX reporting enabled.
     * @return true if JMX reporting enabled
     */
    public final boolean isJmxReportingEnabled()
    {
        return myJmxReportingConfig != null && myJmxReportingConfig.isEnabled();
    }

    /**
     * Returns whether file reporting enabled.
     * @return true if file reporting enabled
     */
    public final boolean isFileReportingEnabled()
    {
        return myFileReportingConfig != null && myFileReportingConfig.isEnabled();
    }
}
