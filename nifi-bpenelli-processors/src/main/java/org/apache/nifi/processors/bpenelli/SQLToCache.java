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

import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"sql", "cache", "map", "statement", "query", "database", "bpenelli"})
@CapabilityDescription("Saves key/value pairs returned from a SQL query to map cache. "
        + "The value from column 0 will be used for the map cache entry key name, and the value "
        + "from column 1 will be used for the map cache entry value. If column 2 is present, "
        + "then a second map cache entry key will be updated with the same value as the first.")
@SeeAlso()
@WritesAttributes({@WritesAttribute(attribute = "sql.failure.reason", description = "The reason the FlowFile was sent to failue relationship.")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class SQLToCache extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile which was successfully created.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile whose SQL failed.")
            .build();

    public static final PropertyDescriptor SQL_TEXT = new PropertyDescriptor.Builder()
            .name("SQL")
            .description("The SQL statement(s) to execute. If left empty the FlowFile contents will be used instead.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller service to use to obtain a database connection.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
            .name("Distributed Map Cache Service")
            .description("Map Cache Controller Service")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .addValidator(Validator.VALID)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SQL_TEXT);
        descriptors.add(DBCP_SERVICE);
        descriptors.add(CACHE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**************************************************************
     * getRelationships
     **************************************************************/
    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    /**************************************************************
     * getSupportedPropertyDescriptors
     **************************************************************/
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    /**************************************************************
     * onScheduled
     **************************************************************/
    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    /**************************************************************
     * onTrigger
     **************************************************************/
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        String sqlText = context.getProperty(SQL_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);

        final Connection conn = dbcpService.getConnection();
        final Sql sql = new Sql(conn);

        if (sqlText == null || sqlText.length() == 0) {
            // Read content.
            sqlText = FlowUtils.readContent(session, flowFile).get();
        }

        try {
            List<GroovyRowResult> data = sql.rows(sqlText);
            for (GroovyRowResult row : data) {
                Object col0 = row.getAt(0);
                Object col1 = row.getAt(1);
                Object col2 = row.getAt(2);
                Object key = FlowUtils.getColValue(col0, "");
                Object value = FlowUtils.getColValue(col1, "");
                cacheService.put(key.toString(), value.toString(), FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                if (col2 != null) {
                    key = FlowUtils.getColValue(col2, "");
                    cacheService.put(key.toString(), value.toString(), FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                }
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (SQLException | IOException e) {
            flowFile = session.putAttribute(flowFile, "sql.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            sql.close();
        }
    }
}