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
package org.bpenelli.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("StringBufferReplaceableByString")
public class ConvertJSONToCSVTest {

    /**
     * Test of onTrigger method, of class GoGetter.
     */
    @org.junit.Test
    public void testOnTrigger() {
        // Add content.   	
        InputStream content = new ByteArrayInputStream("[\"{\\\"a\\\": 1, \\\"b\\\": \\\"two\\\"}\"]".getBytes());

        StringBuilder schema = new StringBuilder();
        schema.append("{");
        schema.append("\"type\": \"record\",");
        schema.append("\"name\": \"data\",");
        schema.append("\"fields\": [{");
        schema.append("\"name\": \"a\",");
        schema.append("\"type\": [\"long\",");
        schema.append("\"null\"]");
        schema.append("},");
        schema.append("{");
        schema.append("\"name\": \"b\",");
        schema.append("\"type\": [\"string\",");
        schema.append("\"null\"]");
        schema.append("}]}");

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new ConvertJSONToCSV());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(ConvertJSONToCSV.SCHEMA, schema.toString());

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ConvertJSONToCSV.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test assertions.
        //result.assertAttributeEquals("test.result", "Good!");
        result.assertContentEquals("\"a\",\"b\"\n\"1\",\"two\"");
    }

}