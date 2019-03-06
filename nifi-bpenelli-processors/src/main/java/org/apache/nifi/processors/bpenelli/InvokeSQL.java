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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"invoke", "sql", "statement", "DML", "database", "bpenelli"})
@CapabilityDescription("Invokes one or more SQL statements against a database connection.")
@SeeAlso()
@WritesAttributes({
        @WritesAttribute(attribute = "sql.failure.reason", description = "The reason the FlowFile was sent to failue relationship."),
        @WritesAttribute(attribute = "sql.failure.sql", description = "The SQL that caused the FlowFile to be sent to failue relationship.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class InvokeSQL extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile whose SQL was successfully invoked")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile whose SQL cannot be invoked")
            .build();

    public static final PropertyDescriptor SQL_TEXT = new PropertyDescriptor.Builder()
            .name("SQL")
            .description("The SQL statement(s) to execute. If left empty the FlowFile contents will be used instead.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor DELIM = new PropertyDescriptor.Builder()
            .name("Multi-statement Delimiter")
            .description("Multi-statement delimiter used.")
            .required(true)
            .defaultValue(";")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor BLOCK_ON_ERROR = new PropertyDescriptor.Builder()
            .name("Block on Error")
            .description("Determines whether or not to block flow files from continuing on an error. "
                    + "If false, flow files will be sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller service to use to obtain a database connection.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
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
        descriptors.add(DELIM);
        descriptors.add(BLOCK_ON_ERROR);
        descriptors.add(DBCP_SERVICE);
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

    private String currentStatement;

    /**************************************************************
     * onTrigger
     **************************************************************/
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        String sqlText = context.getProperty(SQL_TEXT).evaluateAttributeExpressions(flowFile).getValue();
        final String delimiter = context.getProperty(DELIM).evaluateAttributeExpressions(flowFile).getValue();
        final boolean blockOnError = context.getProperty(BLOCK_ON_ERROR).asBoolean();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final Connection conn = dbcpService.getConnection();
        final Sql sql = new Sql(conn);

        if (sqlText == null || sqlText.length() == 0) {
            // Read content.
            sqlText = FlowUtils.readContent(session, flowFile).get();
        }

        final String[] statements = sqlText.split(delimiter);
        try {
            for (String statement : statements) {
                if (statement == null || statement.length() == 0) continue;
                this.currentStatement = statement;
                sql.execute(statement);
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (SQLException e) {
            if (blockOnError) {
                throw new ProcessException(e);
            } else {
                flowFile = session.putAttribute(flowFile, "sql.failure.reason", e.getMessage());
                if (this.currentStatement != null) {
                    flowFile = session.putAttribute(flowFile, "sql.failure.sql", this.currentStatement);
                }
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            sql.close();
        }
    }
}