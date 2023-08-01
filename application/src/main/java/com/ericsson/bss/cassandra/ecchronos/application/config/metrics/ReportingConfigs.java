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

public class ReportingConfigs
{
    private ReportingConfig myJmxReportingConfig = new ReportingConfig();
    private ReportingConfig myFileReportingConfig = new ReportingConfig();
    private ReportingConfig myHttpReportingConfig = new ReportingConfig();

    @JsonProperty("jmx")
    public final ReportingConfig getJmxReportingConfig()
    {
        return myJmxReportingConfig;
    }

    @JsonProperty("file")
    public final ReportingConfig getFileReportingConfig()
    {
        return myFileReportingConfig;
    }

    @JsonProperty("http")
    public final ReportingConfig getHttpReportingConfig()
    {
        return myHttpReportingConfig;
    }

    @JsonProperty("jmx")
    public final void setJmxReportingConfig(final ReportingConfig jmxReportingConfig)
    {
        myJmxReportingConfig = jmxReportingConfig;
    }

    @JsonProperty("file")
    public final void setFileReportingConfig(final ReportingConfig fileReportingConfig)
    {
        myFileReportingConfig = fileReportingConfig;
    }

    @JsonProperty("http")
    public final void setHttpReportingConfig(final ReportingConfig httpReportingConfig)
    {
        myHttpReportingConfig = httpReportingConfig;
    }

    public final boolean isHttpReportingEnabled()
    {
        return myHttpReportingConfig != null && myHttpReportingConfig.isEnabled();
    }

    public final boolean isJmxReportingEnabled()
    {
        return myJmxReportingConfig != null && myJmxReportingConfig.isEnabled();
    }

    public final boolean isFileReportingEnabled()
    {
        return myFileReportingConfig != null && myFileReportingConfig.isEnabled();
    }
}
