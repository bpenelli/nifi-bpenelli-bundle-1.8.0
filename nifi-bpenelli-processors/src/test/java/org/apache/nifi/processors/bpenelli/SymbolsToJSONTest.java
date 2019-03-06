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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SymbolsToJSONTest {

    /**
     * Test of onTrigger method, of class AttributesToJSON.
     */
    @org.junit.Test
    public void testOnTrigger() {

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new SymbolsToJSON());

        runner.setValidateExpressionUsage(false);

        // Create Attributes.
        Map<String, String> attributes = new HashMap<>();
        attributes.put("pre.b", "2");
        attributes.put("a", "1");
        attributes.put("d", "4");
        attributes.put("c", "3");

        // Add properties.
        runner.setProperty(SymbolsToJSON.ATT_PREFIX, "pre.");
        runner.setProperty(SymbolsToJSON.STRIP_PREFIX, "true");
        runner.setProperty(SymbolsToJSON.FILTER_REGEX, "^c$");
        runner.setProperty(SymbolsToJSON.ATT_LIST, "a,d");

        // Add the content to the runner.
        runner.enqueue("", attributes);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(SymbolsToJSON.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test content.
        result.assertContentEquals("{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\",\"d\":\"4\"}");
    }
}