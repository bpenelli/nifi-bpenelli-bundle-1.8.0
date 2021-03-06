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
import java.util.List;

import static org.junit.Assert.assertEquals;

public class XMLToAttributesTest {

    /**
     * Test of onTrigger method, of class XMLToAttributesTest.
     */
    @org.junit.Test
    public void testOnTrigger() {

        // Add content.
        InputStream content = new ByteArrayInputStream("<root><my.name>Dude</my.name></root>".getBytes());

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new XMLToAttributes());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(XMLToAttributes.XML_ROOT, "/root");
        runner.setProperty(XMLToAttributes.PARSE_TYPE, "record");
        runner.setProperty(XMLToAttributes.ATTRIBUTE_PREFIX, "test.");

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(XMLToAttributes.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test attributes and content.
        result.assertAttributeEquals("test.my.name", "Dude");

        // Test 2

        // Add content.
        content = new ByteArrayInputStream("<root><names><my.name>Dude</my.name></names></root>".getBytes());

        // Generate a test runner to mock a processor in a flow.
        runner = TestRunners.newTestRunner(new XMLToAttributes());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(XMLToAttributes.XML_ROOT, "/root");
        runner.setProperty(XMLToAttributes.PARSE_TYPE, "table");
        runner.setProperty(XMLToAttributes.ATTRIBUTE_PREFIX, "test.");

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        results = runner.getFlowFilesForRelationship(XMLToAttributes.REL_SUCCESS);
        assertEquals("1 success", 1, results.size());
        result = results.get(0);

        // Test attributes and content.
        result.assertAttributeEquals("test.my.name", "Dude");

        results = runner.getFlowFilesForRelationship(XMLToAttributes.REL_ORIGINAL);
        assertEquals("1 original", 1, results.size());

    }

}