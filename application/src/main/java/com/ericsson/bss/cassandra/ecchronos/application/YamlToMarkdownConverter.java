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
package com.ericsson.bss.cassandra.ecchronos.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class YamlToMarkdownConverter
{

    private static final Logger LOG = LoggerFactory.getLogger(YamlToMarkdownConverter.class);
    private static final String START_YAML_MARKER = "###";

    private YamlToMarkdownConverter()
    {
    }

    public static void main(final String[] args)
    {
        final String relativePathToDocs = "docs/autogenerated";
        final String outputFileName = "ECCHRONOS_SETTINGS.md";
        final String currentWorkingDir = System.getProperty("user.dir");
        String inputFilename = "/ecc.yml";

        String outFilename = Paths.get(currentWorkingDir, relativePathToDocs, outputFileName).toString();

        try
        {
            convertFile(inputFilename, outFilename);
        }
        catch (IOException e)
        {
            LOG.error("Error converting Ecc YAML file to markdown file", e);
        }
    }

    private static void convertFile(final String inFilename, final String outFilename) throws IOException
    {
        List<String> lines;
        try (InputStream inputStream = YamlToMarkdownConverter.class.getResourceAsStream(inFilename);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)))
        {

            lines = reader.lines().collect(Collectors.toList());
        }

        List<String> filteredLines = new ArrayList<>();
        boolean isLicenseHeader = true;

        for (String line : lines)
        {
            if (isLicenseHeader)
            {
                if (line.trim().startsWith(START_YAML_MARKER))
                {
                    isLicenseHeader = false;
                }
                else
                {
                    continue; // Skip license header lines
                }
            }
            filteredLines.add(line);
        }

        Path outputPath = Paths.get(outFilename);
        Files.write(outputPath, formatAsMarkdown(filteredLines));
    }

    private static List<String> formatAsMarkdown(final List<String> lines)
    {
        List<String> markdownLines = new ArrayList<>();
        markdownLines.add("```yaml");
        markdownLines.addAll(lines);
        markdownLines.add("```");
        return markdownLines;
    }
}
