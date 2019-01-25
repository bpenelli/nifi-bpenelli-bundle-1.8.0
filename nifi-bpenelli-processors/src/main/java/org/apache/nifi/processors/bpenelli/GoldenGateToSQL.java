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

import groovy.json.JsonSlurper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"goldengate, sql, trail, json, bpenelli"})
@CapabilityDescription("Parses an Oracle GoldenGate trail file and builds a corresponding SQL statement.")
@SeeAlso()
public class GoldenGateToSQL extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("Trail File Format")
            .description("The format of the trail file.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .allowableValues("JSON")
            .defaultValue("JSON")
            .build();

    public static final PropertyDescriptor TO_CASE = new PropertyDescriptor.Builder()
            .name("Convert Case")
            .description("Convert table and column name case.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .allowableValues("None", "Upper", "Lower")
            .defaultValue("None")
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Target Schema")
            .description("The target schema name. Only needed if overriding the one in the trail file.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SEMICOLON = new PropertyDescriptor.Builder()
            .name("Include Semicolon")
            .description("If true, a semicolon will be added to the end of the SQL statement.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("SQL Attribute")
            .description("The name of the attribute to output the SQL to. If left empty, it will be written to the FlowFile's content.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor KEYCOLS = new PropertyDescriptor.Builder()
            .name("Key Columns")
            .description("Comma separated list of key column names. Only needed if the primary_keys property is not in the trail file, or to override.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final String colMapDesc = "Ex: SRCTBL.CUST_ID | CUSTOMER_ID ";

    public static final PropertyDescriptor COLMAP = new PropertyDescriptor.Builder()
            .name("*** Add Column Mapping Below ***")
            .description("For column mapping, add dynamic properties, i.e. PropertyName: <SrcTableName>.<SrcColName>, PropertyValue: <TargColName>.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .allowableValues(colMapDesc)
            .defaultValue(colMapDesc)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FORMAT);
        descriptors.add(TO_CASE);
        descriptors.add(SCHEMA);
        descriptors.add(SEMICOLON);
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(COLMAP);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
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
    @SuppressWarnings({"unchecked"})
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        final AtomicReference<Map<String, Object>> content = new AtomicReference<>();
        content.set((Map<String, Object>) new JsonSlurper().parseText(FlowUtils.readContent(session, flowFile).get()));

        int i = 0;
        final StringBuilder sql = new StringBuilder();
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        final boolean includeSemicolon = context.getProperty(SEMICOLON).asBoolean();
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions().getValue();
        final String keyCols = context.getProperty(KEYCOLS).evaluateAttributeExpressions().getValue();
        final String toCase = context.getProperty(TO_CASE).getValue();
        String[] pk = new String[0];

        if (keyCols != null && keyCols.length() > 0) {
            pk = keyCols.split(",");
        } else if (content.get().containsKey("primary_keys")) {
            pk = ((ArrayList<String>) content.get().get("primary_keys")).toArray(pk);
        }

        String table = content.get().get("table").toString();
        final String tableName = table.substring(table.indexOf(".") + 1);
        final String opType = content.get().get("op_type").toString();
        final Map<String, Object> before = (Map<String, Object>) content.get().get("before");
        final Map<String, Object> after = (Map<String, Object>) content.get().get("after");

        if (schema != null) {
            if (schema.length() > 0) {
                table = schema + table.substring(table.indexOf("."));
            } else {
                table = tableName;
            }
        }
        table = FlowUtils.applyCase(table, toCase);

        // Build the SQL
        if (opType.equals("I")) {
            // Insert statement
            final StringBuilder cols = new StringBuilder("(");
            final StringBuilder vals = new StringBuilder("VALUES (");
            sql.append("INSERT INTO ").append(table).append(" ");
            for (String item : after.keySet()) {
                final String colName = FlowUtils.applyColMap(context, flowFile, tableName, item, toCase);
                if (i > 0) {
                    cols.append(", ");
                    vals.append(", ");
                }
                cols.append(colName);
                final Object value = after.get(item);
                if (value != null) {
                    final String val = value.toString().replace("'", "''");
                    vals.append("'").append(val).append("'");
                } else {
                    vals.append("null");
                }
                i++;
            }
            cols.append(")");
            vals.append(")");
            sql.append(cols).append(" ").append(vals);
        } else {
            if (pk.length == 0) {
                throw new ProcessException("Primary key column(s) are required for this operation.");
            }
            if (opType.equals("U")) {
                // Update statement
                sql.append("UPDATE ").append(table).append(" SET ");
                for (final String col : after.keySet()) {
                    boolean isPk = false;
                    for (String p : pk) {
                        if (col.equals(p)) {
                            isPk = true;
                            break;
                        }
                    }
                    if (!isPk) {
                        final String colName = FlowUtils.applyColMap(context, flowFile, tableName, col, toCase);
                        if (i > 0) {
                            sql.append(", ");
                        }
                        sql.append(colName).append(" = ");
                        final Object colValue = after.get(col);
                        if (colValue != null) {
                            String val = colValue.toString().replace("'", "''");
                            sql.append("'").append(val).append("'");
                        } else {
                            sql.append("null");
                        }
                        i++;
                    }
                }
            } else if (opType.equals("D")) {
                // Delete statement
                sql.append("DELETE FROM ").append(table);
            }
            // Where clause for the update or delete.
            i = 0;
            sql.append(" WHERE ");
            for (final String col : pk) {
                final String colName = FlowUtils.applyColMap(context, flowFile, tableName, col, toCase);
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(colName).append(" ");
                final Object colValue = before.get(col);
                if (colValue != null) {
                    final String val = colValue.toString().replace("'", "''");
                    sql.append("= '").append(val).append("'");
                } else {
                    sql.append("IS null");
                }
                i++;
            }
        }

        if (includeSemicolon) sql.append(";");

        // Output the SQL
        if (attName != null && !attName.isEmpty()) {
            flowFile = session.putAttribute(flowFile, attName, sql.toString());
        } else {
            flowFile = FlowUtils.writeContent(session, flowFile, sql.toString());
        }

        // Success!
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();

    }
}
