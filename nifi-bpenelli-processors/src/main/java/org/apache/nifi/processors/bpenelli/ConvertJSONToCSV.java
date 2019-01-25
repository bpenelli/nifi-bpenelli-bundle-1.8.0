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
import groovy.json.internal.ValueList;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static groovy.json.JsonParserType.LAX;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"convert, json, csv, schema, bpenelli"})
@CapabilityDescription("Converts JSON data to CSV data. The JSON must be an object or an array, "
        + "if an array, the array elements must be JSON objects or strings that define JSON objects.")
@SeeAlso()
public class ConvertJSONToCSV extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully converted")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be converted")
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .description("The schema to use to map JSON to CSV.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor DELIM = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .description("The delimiter to use.")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    @SuppressWarnings("WeakerAccess")
    public static final PropertyDescriptor COL_HEADERS = new PropertyDescriptor.Builder()
            .name("Column Headers")
            .description("If true, will output column headers.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(false)
            .addValidator(Validator.VALID)
            .build();

    @SuppressWarnings("WeakerAccess")
    public static final PropertyDescriptor QUOTED = new PropertyDescriptor.Builder()
            .name("Quoted Fields")
            .description("If true, will output quoted fields.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(false)
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
        descriptors.add(SCHEMA);
        descriptors.add(DELIM);
        descriptors.add(COL_HEADERS);
        descriptors.add(QUOTED);
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
    @SuppressWarnings("unchecked")
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        final String delim = context.getProperty(DELIM).evaluateAttributeExpressions(flowFile).getValue();
        final boolean headers = context.getProperty(COL_HEADERS).asBoolean();
        final boolean quoted = context.getProperty(QUOTED).asBoolean();

        final AtomicReference<Object> rawData = new AtomicReference<>();
        AtomicReference<ValueList> jsonData = new AtomicReference<>(new ValueList(false));
        final String ELEMENT_TYPE_ERROR = "Array elements must contain JSON objects or strings that define JSON objects.";

        // Read content.
        session.read(flowFile, inputStream -> rawData.set(new JsonSlurper().setType(LAX).parseText(IOUtils.toString(inputStream, java.nio.charset.StandardCharsets.UTF_8))));

        if (rawData.get() instanceof ValueList) {
            jsonData.set((ValueList) rawData.get());
        } else if (rawData.get() instanceof Map) {
            jsonData.get().add(rawData.get());
        } else if (rawData.get() instanceof ArrayList) {
            FlowUtils.writeContent(session, flowFile, "");
            session.transfer(flowFile, REL_SUCCESS);
            return;
        }

        final Map<String, Object> schemaData = (Map<String, Object>) new JsonSlurper().setType(LAX).parseText(schema);
        final ValueList fieldList = (ValueList) schemaData.get("fields");
        final StringBuilder csv = new StringBuilder();
        boolean isFirstLine = true;
        boolean isFirstCol = true;

        try {

            // Add CSV headers if requested.
            if (headers) {
                for (Object item : fieldList) {
                    final Map<String, Object> field = (Map<String, Object>) item;
                    if (!isFirstCol) csv.append(delim);
                    if (quoted) csv.append("\"");
                    csv.append(field.get("name").toString());
                    if (quoted) csv.append("\"");
                    isFirstCol = false;
                }
                isFirstLine = false;
            }

            // Add CSV data lines.
            while (jsonData.get().size() > 0) {
                final Object rawRecord = jsonData.get().get(0);
                Map<String, Object> record;
                if (rawRecord instanceof String) {
                    // See if the string contains a JSON object.
                    final Object checkType = new JsonSlurper().setType(LAX).parseText(rawRecord.toString());
                    if (checkType instanceof Map) {
                        record = (Map<String, Object>) checkType;
                    } else {
                        throw new IllegalArgumentException(ELEMENT_TYPE_ERROR);
                    }
                } else if (rawRecord instanceof Map) {
                    record = (Map<String, Object>) rawRecord;
                } else {
                    throw new IllegalArgumentException(ELEMENT_TYPE_ERROR);
                }
                if (!isFirstLine) csv.append("\n");
                isFirstCol = true;
                for (final Object item : fieldList) {
                    final Map<String, Object> field = (Map<String, Object>) item;
                    final String fieldName = field.get("name").toString();
                    if (!isFirstCol) csv.append(delim);
                    if (quoted) csv.append("\"");
                    if (record.containsKey(fieldName)) {
                        String fieldValue = record.get(fieldName).toString();
                        if (quoted) fieldValue = fieldValue.replace("\"", "\"\"");
                        csv.append(fieldValue);
                    }
                    if (quoted) csv.append("\"");
                    isFirstCol = false;
                }
                isFirstLine = false;
                jsonData.get().remove(rawRecord);
            }

            // Write CSV to the FlowFile's content.
            flowFile = FlowUtils.writeContent(session, flowFile, csv.toString());

            session.transfer(flowFile, REL_SUCCESS);

        } catch (final IllegalArgumentException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to convert {} due to {}", new Object[]{flowFile, e});
        }
    }
}