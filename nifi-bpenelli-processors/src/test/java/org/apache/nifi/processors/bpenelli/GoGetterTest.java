/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.bpenelli;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GoGetterTest {

    /**
     * Test of onTrigger method, of class GoGetter.
     */
    @org.junit.Test
    public void testOnTrigger() {
        // Add content.
        InputStream content = new ByteArrayInputStream("Hello World!".getBytes());

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new GoGetter());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(GoGetter.GOG_TEXT,
                "{"
                        + "\"extract-to-attributes\":{"
                        + "\"test.result\":\"Good!\""
                        + "},"
                        + "\"extract-to-json\":{"
                        + "\"test.result\":\"Hello World!\","
                        + "\"test.no\":{"
                        + "\"value\":\"21020\","
                        + "\"to-type\":\"long\"},"
                        + "\"test.exp\":{"
                        + "\"value\":\"${exp}\","
                        + "\"to-type\":\"long\"},"
                        + "\"null.exp\":{"
                        + "\"value\":null,"
                        + "\"default\":\"-1\","
                        + "\"to-type\":\"long\"}"
                        + "}"
                        + "}");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("exp", "2");

        // Add the content to the runner.
        runner.enqueue(content, attributes);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GoGetter.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test attributes and content.
        result.assertAttributeEquals("test.result", "Good!");
        result.assertContentEquals("{\"null.exp\":-1,\"test.exp\":2,\"test.no\":21020,\"test.result\":\"Hello World!\"}");
    }

}