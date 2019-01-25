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
public class GoldenGateToSQLTest {

    /**
     * Test of onTrigger method, of class GoldenGateToSQL.
     */
    @org.junit.Test
    public void testOnTrigger() {

        StringBuilder trail = new StringBuilder();
        trail.append("{");
        trail.append("\"table\": \"MOCK.MYTABLE\",");
        trail.append("\"op_type\": \"U\",");
        trail.append("\"op_ts\": \"2018-04-12 00:56:35.015432\",");
        trail.append("\"current_ts\": \"2018-04-12T00:57:04.102000\",");
        trail.append("\"pos\": \"00000000000000003064\",");
        trail.append("\"primary_keys\": [\"PK1_ID\",");
        trail.append("\"PK2_ID\",");
        trail.append("\"PK3_ID\"],");
        trail.append("\"before\": {");
        trail.append("\"PK1_ID\": 1,");
        trail.append("\"PK2_ID\": 2,");
        trail.append("\"PK3_ID\": 3,");
        trail.append("\"SOURCE_LANG\": \"US\",");
        trail.append("\"NAME\": \"Name Before Update\",");
        trail.append("\"LAST_UPDATE_DATE\": \"2017-09-22 10:56:35\",");
        trail.append("\"LAST_UPDATED_BY\": 1433,");
        trail.append("\"LAST_UPDATE_LOGIN\": 333912,");
        trail.append("\"CREATED_BY\": 1156,");
        trail.append("\"CREATION_DATE\": \"2017-09-19 11:16:25\",");
        trail.append("\"ZD_SYNC\": \"SYNCED\"");
        trail.append("},");
        trail.append("\"after\": {");
        trail.append("\"PK1_ID\": 1,");
        trail.append("\"PK2_ID\": 2,");
        trail.append("\"PK3_ID\": 3,");
        trail.append("\"NAME\": \"Name After Update\",");
        trail.append("\"ZD_SYNC\": \"SYNCED\"");
        trail.append("}");
        trail.append("}");


        // Add content.
        InputStream content = new ByteArrayInputStream(trail.toString().getBytes());

        // Generate a test runner to mock a processor in a flow.
        TestRunner runner = TestRunners.newTestRunner(new GoldenGateToSQL());

        runner.setValidateExpressionUsage(false);

        // Add properties.
        runner.setProperty(GoldenGateToSQL.FORMAT, "JSON");
        runner.setProperty(GoldenGateToSQL.TO_CASE, "Lower");
        runner.setProperty(GoldenGateToSQL.SCHEMA, "gg");

        // Add the content to the runner.
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued.
        runner.run(1);

        // All results were processed with out failure.
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content.
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GoldenGateToSQL.REL_SUCCESS);
        assertEquals("1 match", 1, results.size());
        MockFlowFile result = results.get(0);

        // Test attributes and content.
        result.assertContentEquals("UPDATE gg.mytable SET name = 'Name After Update', zd_sync = 'SYNCED' WHERE pk1_id = '1' AND pk2_id = '2' AND pk3_id = '3'");
    }

}