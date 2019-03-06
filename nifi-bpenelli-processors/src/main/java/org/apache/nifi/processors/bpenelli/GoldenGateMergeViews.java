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

import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"goldengate, merge, trail, json, bpenelli"})
@CapabilityDescription("Merges the before and after views of an Oracle GoldenGate trail file to create a merged view, "
        + "and outputs it as JSON. Only op_types \"I\", \"U\", and \"D\" are supported. FlowFiles with other op_types "
        + "will be routed to unsupported_op_type relationship. Any defined dynamic property will be included in the "
        + "JSON.")
@SeeAlso()
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class GoldenGateMergeViews extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_UNSUPPORTED = new Relationship.Builder()
            .name("unsupported op_type")
            .description("FlowFiles containing an unsupported Golden Gate op_type")
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

    public static final PropertyDescriptor GG_FIELDS = new PropertyDescriptor.Builder()
            .name("Include Golden Gate Fields")
            .description("A comma delimited list of Golden Gate single value header fields that should be added to "
                    + "the final JSON.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Output Attribute Name")
            .description("The name of the attribute to output the JSON to. If left empty, it will be written to the "
                    + "FlowFile's content.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        descriptors.add(FORMAT);
        descriptors.add(TO_CASE);
        descriptors.add(SCHEMA);
        descriptors.add(GG_FIELDS);
        descriptors.add(ATTRIBUTE_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_UNSUPPORTED);
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
     * getSupportedDynamicPropertyDescriptor
     **************************************************************/
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
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

        // Read the FlowFile's contents in.
        final AtomicReference<Map<String, Object>> content = new AtomicReference<>();
        content.set((Map<String, Object>) (new JsonSlurper()).parseText(FlowUtils.readContent(session, flowFile).get()));

        // Verify it's a supported op_type, i.e. Insert or Update.
        final String opType = content.get().get("op_type").toString();
        if (!opType.equals("I") && !opType.equals("U") && !opType.equals("D")) {
            // Unsupported
            session.transfer(flowFile, REL_UNSUPPORTED);
            session.commit();
            return;
        }

        // Get property values.
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        final String ggFieldsCSV = context.getProperty(GG_FIELDS).getValue();
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions().getValue();
        final String toCase = context.getProperty(TO_CASE).getValue();

        String table = content.get().get("table").toString();
        final String tableName = table.substring(table.indexOf(".") + 1);
        final Map<String, Object> before = (Map<String, Object>) content.get().get("before");
        final Map<String, Object> after = (Map<String, Object>) content.get().get("after");
        final Map<String, Object> jsonMap = new TreeMap<>();

        content.get().remove("primary_keys");
        content.get().remove("before");
        content.get().remove("after");

        String[] ggFields = null;

        if (ggFieldsCSV != null) {
            ggFields = ggFieldsCSV.split(",");
        }

        if (schema != null) {
            if (schema.length() > 0) {
                table = schema + table.substring(table.indexOf("."));
            } else {
                table = tableName;
            }
        }

        table = FlowUtils.applyCase(table, toCase);

        if (ggFields != null && ggFields.length > 0) {
            for (final String field : ggFields) {
                for (final String key : content.get().keySet()) {
                    if (field.equals(key)) {
                        Object val = content.get().get(key);
                        if (!(val instanceof Map) && !(val instanceof ArrayList)) {
                            if (key.equals("table")) val = table;
                            jsonMap.put(FlowUtils.applyCase(key, toCase), val);
                        }
                        break;
                    }
                }
            }
        }

        if (before != null) {
            for (final String key : before.keySet()) {
                jsonMap.put(FlowUtils.applyCase(key, toCase), before.get(key));
            }
        }

        if (after != null) {
            for (final String key : after.keySet()) {
                jsonMap.put(FlowUtils.applyCase(key, toCase), after.get(key));
            }
        }

        // Get dynamic properties.
        for (PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (propDesc.isDynamic()) {
                PropertyValue propVal = context.getProperty(propDesc);
                jsonMap.put(propDesc.getName(), propVal.evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        // Build a JSON object for these results and put it in the FlowFile's content.
        final JsonBuilder builder = new JsonBuilder();
        builder.call(jsonMap);

        // Output the JSON
        if (attName != null && !attName.isEmpty()) {
            flowFile = session.putAttribute(flowFile, attName, builder.toString());
        } else {
            flowFile = FlowUtils.writeContent(session, flowFile, builder.toString());
        }

        // Success!
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();

    }
}
