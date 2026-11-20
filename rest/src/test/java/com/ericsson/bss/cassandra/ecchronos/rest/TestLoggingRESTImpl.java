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
package com.ericsson.bss.cassandra.ecchronos.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Tests {@link LoggingRESTImpl} against the real logback via Spring Boot's {@link LoggingSystem}, so that a level
 * change is verified on the actual logger and not only on a mock.
 */
public class TestLoggingRESTImpl
{
    private static final String PARENT = "ecc.test.logging";
    private static final String CHILD = PARENT + ".Child";

    private LoggingRESTImpl myController;
    private ch.qos.logback.classic.Logger myRoot;
    private Level myOriginalRootLevel;

    @Before
    public void setup()
    {
        ILoggerFactory factory = LoggerFactory.getILoggerFactory();
        assertThat(factory).as("these tests need real logback").isInstanceOf(LoggerContext.class);

        myRoot = ((LoggerContext) factory).getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        myOriginalRootLevel = myRoot.getLevel();
        myController = new LoggingRESTImpl(LoggingSystem.get(getClass().getClassLoader()));
    }

    @After
    public void restore()
    {
        myRoot.setLevel(myOriginalRootLevel);
        logger(PARENT).setLevel(null);
        logger(CHILD).setLevel(null);
    }

    @Test
    public void testSetLevelAppliesToRealLogger()
    {
        ResponseEntity<Map<String, Object>> response = myController.setLogger(PARENT, level("DEBUG"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(logger(PARENT).getLevel()).isEqualTo(Level.DEBUG);
        assertThat(logger(PARENT).isDebugEnabled()).isTrue();
        assertThat(response.getBody()).containsEntry("logger", PARENT)
                .containsEntry("configured_level", "DEBUG")
                .containsEntry("effective_level", "DEBUG");
    }

    @Test
    public void testSetEachAcceptedLevel()
    {
        Map<String, Level> expected = new HashMap<>();
        expected.put("TRACE", Level.TRACE);
        expected.put("DEBUG", Level.DEBUG);
        expected.put("INFO", Level.INFO);
        expected.put("WARN", Level.WARN);
        expected.put("ERROR", Level.ERROR);
        expected.put("OFF", Level.OFF);

        for (Map.Entry<String, Level> entry : expected.entrySet())
        {
            ResponseEntity<Map<String, Object>> response = myController.setLogger(PARENT, level(entry.getKey()));

            assertThat(response.getStatusCode()).as(entry.getKey()).isEqualTo(HttpStatus.OK);
            assertThat(logger(PARENT).getLevel()).as(entry.getKey()).isEqualTo(entry.getValue());
        }
    }

    @Test
    public void testLevelIsCaseInsensitive()
    {
        myController.setLogger(PARENT, level("warn"));

        assertThat(logger(PARENT).getLevel()).isEqualTo(Level.WARN);
    }

    @Test
    public void testResetClearsExplicitLevelAndInheritsAgain()
    {
        myRoot.setLevel(Level.INFO);
        myController.setLogger(PARENT, level("TRACE"));

        ResponseEntity<Map<String, Object>> response = myController.setLogger(PARENT, level("reset"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(logger(PARENT).getLevel()).isNull();
        assertThat(logger(PARENT).getEffectiveLevel()).isEqualTo(Level.INFO);
        assertThat(response.getBody()).containsEntry("configured_level", null)
                .containsEntry("effective_level", "INFO");
    }

    @Test
    public void testResetIsCaseInsensitive()
    {
        myController.setLogger(PARENT, level("DEBUG"));

        myController.setLogger(PARENT, level("RESET"));

        assertThat(logger(PARENT).getLevel()).isNull();
    }

    @Test
    public void testChildInheritsFromParentUntilItHasItsOwnLevel()
    {
        myController.setLogger(PARENT, level("DEBUG"));

        Map<String, Object> inherited = myController.getLogger(CHILD).getBody();
        assertThat(inherited).containsEntry("configured_level", null).containsEntry("effective_level", "DEBUG");

        myController.setLogger(CHILD, level("ERROR"));

        Map<String, Object> own = myController.getLogger(CHILD).getBody();
        assertThat(own).containsEntry("configured_level", "ERROR").containsEntry("effective_level", "ERROR");
    }

    @Test
    public void testGetUnknownLoggerReportsInheritedEffectiveLevel()
    {
        myRoot.setLevel(Level.WARN);

        ResponseEntity<Map<String, Object>> response = myController.getLogger("never.created.Logger");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsEntry("logger", "never.created.Logger")
                .containsEntry("configured_level", null)
                .containsEntry("effective_level", "WARN");
    }

    @Test
    public void testRootCanBeReadAndSet()
    {
        myController.setLogger("ROOT", level("ERROR"));

        assertThat(myRoot.getLevel()).isEqualTo(Level.ERROR);
        Map<String, Object> body = myController.getLogger("ROOT").getBody();
        assertThat(body).containsEntry("logger", "ROOT")
                .containsEntry("configured_level", "ERROR")
                .containsEntry("effective_level", "ERROR");
    }

    @Test
    public void testRootNameIsCaseInsensitiveAndDoesNotCreateSeparateLogger()
    {
        myRoot.setLevel(Level.INFO);

        ResponseEntity<Map<String, Object>> response = myController.setLogger("root", level("DEBUG"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(myRoot.getLevel()).isEqualTo(Level.DEBUG);
        assertThat(response.getBody()).containsEntry("logger", "ROOT");
    }

    @Test
    public void testRootCannotBeReset()
    {
        myRoot.setLevel(Level.WARN);

        ResponseEntity<Map<String, Object>> response = myController.setLogger("ROOT", level("reset"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsKey("error");
        assertThat(myRoot.getLevel()).isEqualTo(Level.WARN);
    }

    @Test
    public void testInvalidLevelIsRejectedAndNothingChanges()
    {
        myController.setLogger(PARENT, level("DEBUG"));

        ResponseEntity<Map<String, Object>> response = myController.setLogger(PARENT, level("LOUD"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().get("error").toString()).contains("LOUD").contains("TRACE");
        assertThat(logger(PARENT).getLevel()).isEqualTo(Level.DEBUG);
    }

    @Test
    public void testFatalIsRejectedAsLogbackHasNoSuchLevel()
    {
        ResponseEntity<Map<String, Object>> response = myController.setLogger(PARENT, level("FATAL"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(logger(PARENT).getLevel()).isNull();
    }

    @Test
    public void testMissingBodyOrLevelIsRejected()
    {
        assertThat(myController.setLogger(PARENT, null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(myController.setLogger(PARENT, Collections.emptyMap()).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(myController.setLogger(PARENT, level("  ")).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(logger(PARENT).getLevel()).isNull();
    }

    @Test
    public void testNonStringLevelIsRejected()
    {
        Map<String, Object> body = new HashMap<>();
        body.put("level", 5);

        assertThat(myController.setLogger(PARENT, body).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(logger(PARENT).getLevel()).isNull();
    }

    @Test
    public void testEmptyAndMalformedLoggerNamesAreRejected()
    {
        for (String name : new String[] {"", "   ", "bad name", "bad\tname", "bad\nname"})
        {
            assertThat(myController.setLogger(name, level("DEBUG")).getStatusCode())
                    .as("set '%s'", name).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(myController.getLogger(name).getStatusCode())
                    .as("get '%s'", name).isEqualTo(HttpStatus.BAD_REQUEST);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListContainsRootAndExplicitLoggersButNotInheritingOnes()
    {
        logger(CHILD);
        myController.setLogger(PARENT, level("DEBUG"));

        ResponseEntity<Map<String, Object>> response = myController.getLoggers();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> loggers = (List<Map<String, Object>>) response.getBody().get("loggers");
        assertThat(loggers).isNotEmpty();
        assertThat(loggers.get(0)).containsEntry("logger", "ROOT");
        assertThat(loggers).anySatisfy(l -> assertThat(l).containsEntry("logger", PARENT)
                .containsEntry("configured_level", "DEBUG"));
        assertThat(loggers).noneSatisfy(l -> assertThat(l).containsEntry("logger", CHILD));
        assertThat(loggers).allSatisfy(l -> assertThat(l.get("effective_level")).isNotNull());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListDropsLoggerAfterReset()
    {
        myController.setLogger(PARENT, level("DEBUG"));
        myController.setLogger(PARENT, level("reset"));

        List<Map<String, Object>> loggers =
                (List<Map<String, Object>>) myController.getLoggers().getBody().get("loggers");

        assertThat(loggers).noneSatisfy(l -> assertThat(l).containsEntry("logger", PARENT));
    }

    private static Map<String, Object> level(final String level)
    {
        Map<String, Object> body = new HashMap<>();
        body.put("level", level);
        return body;
    }

    private static ch.qos.logback.classic.Logger logger(final String name)
    {
        return (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name);
    }
}
