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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.sql.Connection;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"get", "sql", "scalar", "query", "database", "attribute", "content", "bpenelli"})
@CapabilityDescription("Extracts the value from the first column of the first row returned from a SQL query to a named attribute or to content.")
@WritesAttributes({@WritesAttribute(attribute = "sql.failure.reason", description = "The reason the FlowFile was sent to failue relationship.")})
public class GetSQLScalar extends AbstractProcessor {

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile which was successfully processed.")
            .build();

    public static final Relationship REL_EMPTY = new Relationship.Builder()
            .name("empty")
            .description("Any FlowFile whose SQL returns an empty result set.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile whose SQL failed.")
            .build();

    // Properties
    public static final PropertyDescriptor SQL_TEXT = new PropertyDescriptor.Builder()
            .name("SQL")
            .description("The SQL statement(s) to execute. If left empty the FlowFile contents will be used instead.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATT_NAME = new PropertyDescriptor.Builder()
            .name("Attribute Name")
            .description("The name of an attribute to extract the value to. If left empty, value will be written to content.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor BLOCK_ON_FAILURE = new PropertyDescriptor.Builder()
            .name("Block on Failure")
            .description("Determines whether to block flow files from being sent to failure and remain on the input queue.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller service to use to obtain a database connection.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    // Private Members
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SQL_TEXT);
        descriptors.add(ATT_NAME);
        descriptors.add(BLOCK_ON_FAILURE);
        descriptors.add(DBCP_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_EMPTY);
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
        String attName = context.getProperty(ATT_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final boolean blockOnFailure = context.getProperty(BLOCK_ON_FAILURE).asBoolean();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final Connection conn = dbcpService.getConnection();
        final Sql sql = new Sql(conn);

        if (sqlText == null || sqlText.length() == 0) {
            // Read content.
            sqlText = FlowUtils.readContent(session, flowFile).get();
        }

        try {
            final List<GroovyRowResult> data = sql.rows(sqlText);
            if (data.size() == 0) {
                session.transfer(flowFile, REL_EMPTY);
                return;
            }
            GroovyRowResult row = data.get(0);
            final String value = FlowUtils.getColValue(row.getAt(0), "");
            if (attName != null && !attName.isEmpty()) {
                flowFile = session.putAttribute(flowFile, attName, value);
            } else {
                flowFile = FlowUtils.writeContent(session, flowFile, value);
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error executing SQL due to {}", new Object[]{e});
            if (blockOnFailure) {
                session.rollback();
                return;
            }
            flowFile = session.putAttribute(flowFile, "sql.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            sql.close();
        }
    }
}