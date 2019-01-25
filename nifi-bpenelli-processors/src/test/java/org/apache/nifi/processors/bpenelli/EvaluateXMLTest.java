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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("WeakerAccess")
public class EvaluateXMLTest {

    /**
     * Test of onTrigger method, of class EvaluateXMLTest.
     */
    @org.junit.Test
    public void testOnTrigger() {

        // Add content.
        InputStream content = new ByteArrayInputStream("<root><name>Dude</name></root>".getBytes());

        EvaluateXML proc = new EvaluateXML() {
            public final PropertyDescriptor DYN_PROP_1 = new PropertyDescriptor.Builder()
                    .dynamic(true)
                    .name("extracted_name")
                    .description("XPath to extract the name element from the test XML.")
                    .required(true)
                    .expressionLanguageSupported(true)
                    .addValidator(Validator.VALID)
                    .build();

            @Override
            protected void init(final ProcessorInitializationContext context) {
                final List<PropertyDescriptor> descriptors = new ArrayList<>();
                descriptors.add(ATTRIBUTE_NAME);
                descriptors.add(DYN_PROP_1);
                this.descriptors = Collections.unmodifiableList(descriptors);
                final Set<Relationship> relationships = new HashSet<>();
                relationships.add(REL_SUCCESS);
                relationships.add(REL_FAILURE);
                this.relationships = Collections.unmodifiableSet(relationships);
            }
        };

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(proc);

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty("extracted_name", "/root/${literal(\"name\")}");

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(EvaluateXML.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test attributes
        result.assertAttributeEquals("extracted_name", "Dude");
    }

}