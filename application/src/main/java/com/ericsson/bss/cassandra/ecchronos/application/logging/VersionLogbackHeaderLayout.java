/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.logging;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import ch.qos.logback.classic.PatternLayout;

/**
 * A {@link PatternLayout} that prepends a one-line header identifying the ecChronos version and the
 * git commit the agent was built from to every log file it is attached to.
 *
 * <p>Logback invokes {@link #getFileHeader()} each time an appender opens a file. For a
 * {@code RollingFileAppender} that happens both at startup and on every rollover, so each freshly
 * created log file begins with the header &mdash; which is exactly what makes it possible to tell
 * which build produced a given (possibly archived) log file.</p>
 *
 * <p>The version and commit are read from the build-filtered {@code ecchronos-version.properties}
 * resource on the classpath (the same single source of truth used by the REST version endpoint).
 * The resource name is deliberately unique to avoid colliding with the generic
 * {@code version.properties} that some third-party jars ship. Because this layout runs during
 * logback initialization &mdash; before and independently of the Spring context &mdash; the
 * resource is read directly rather than through Spring property injection. Missing, empty, or
 * unfiltered Maven placeholder values are reported as {@value #UNKNOWN}.</p>
 */
public final class VersionLogbackHeaderLayout extends PatternLayout
{
    static final String UNKNOWN = "unknown";

    private static final String VERSION_RESOURCE = "ecchronos-version.properties";
    private static final String VERSION_PROPERTY = "ecchronos.version";
    private static final String COMMIT_PROPERTY = "ecchronos.git.commit";

    @Override
    public String getFileHeader()
    {
        String version = null;
        String commit = null;

        Properties properties = loadVersionProperties();
        if (properties != null)
        {
            version = properties.getProperty(VERSION_PROPERTY);
            commit = properties.getProperty(COMMIT_PROPERTY);
        }

        return buildHeader(version, commit);
    }

    /**
     * Builds the header line from raw (possibly unfiltered or null) version and commit values.
     * Values are normalized so that missing, empty, or unfiltered Maven placeholders are reported
     * as {@value #UNKNOWN}.
     *
     * @param rawVersion the raw version value, may be null
     * @param rawCommit  the raw commit value, may be null
     * @return the formatted header line
     */
    String buildHeader(final String rawVersion, final String rawCommit)
    {
        String version = normalize(rawVersion);
        String commit = normalize(rawCommit);
        String openedAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        return "# ecChronos " + version + " (commit " + commit + ") - log opened " + openedAt;
    }

    /**
     * Loads {@code ecchronos-version.properties} from the classpath using the thread context
     * class loader. If the resource cannot be located or read (including the unlikely case of a
     * null context class loader), the header degrades to {@value #UNKNOWN} values.
     *
     * @return the parsed properties, or {@code null} if the resource is absent or cannot be read.
     */
    private Properties loadVersionProperties()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
        {
            return null;
        }

        try (InputStream in = classLoader.getResourceAsStream(VERSION_RESOURCE))
        {
            if (in == null)
            {
                return null;
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties;
        }
        catch (IOException e)
        {
            // Never let logging configuration fail the application over a missing/broken header.
            addWarn("Could not read " + VERSION_RESOURCE + " for log file header", e);
            return null;
        }
    }

    /**
     * Normalizes a build-filtered value. Null, empty, and unresolved Maven placeholders (values
     * still starting with {@code ${}) are reported as {@value #UNKNOWN}.
     *
     * @param value the raw value
     * @return the normalized value, never null
     */
    private static String normalize(final String value)
    {
        if (value == null || value.isEmpty() || value.startsWith("${"))
        {
            return UNKNOWN;
        }
        return value;
    }
}
