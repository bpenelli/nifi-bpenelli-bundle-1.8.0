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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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

import static groovy.json.JsonParserType.LAX;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unchecked"})
@Tags({"json", "attributes", "bpenelli"})
@CapabilityDescription("Extracts JSON fields to FlowFile attributes. The JSON can come from the "
        + "FlowFile's content, or a FlowFile attribute. The JSON must represent an object or an array of objects. If "
        + "the JSON is an array of objects, then a new FlowFile will be generated for each object in the array. "
        + "Optionally, new attribute names can be mapped for each of the fields when extracted, otherwise attribute "
        + "names will be the same as the field names. Fields containing JSON objects or arrays are ignored.")
@WritesAttributes(
        {@WritesAttribute(attribute = "json.failure.reason", description = "The reason the FlowFile was sent to failue "
                + "relationship.")}
)
@SeeAlso()
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class JSONToAttributes extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original FlowFiles")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be processed")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute Name")
            .description("The name of the attribute containing source JSON. If left empty the FlowFile's contents "
                    + "will be used.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("Attribute Prefix")
            .description("A prefix to use on all the resulting attribute names.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ADD_NULL_FIELDS = new PropertyDescriptor.Builder()
            .name("Add null Fields")
            .description("If true an attribute will be added for null fields with an empty string value.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ADD_FRAG = new PropertyDescriptor.Builder()
            .name("Add fragment Attributes")
            .description("Select when to add fragment attributes.")
            .required(true)
            .allowableValues("Always", "Never", "JSON Arrays Only")
            .defaultValue("Always")
            .expressionLanguageSupported(NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor NAME_DELIM = new PropertyDescriptor.Builder()
            .name("Name Delimiter")
            .description("Delimiter used to separate names in the \"Attributes to Rename\" and \"New Names\" "
                    + "properties. Defaults to comma.")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("Attributes to Rename")
            .description("Delimited list of field names to change when creating the FlowFile attributes.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor NEW_NAMES = new PropertyDescriptor.Builder()
            .name("New Names")
            .description("Delimited list of new names to apply to the \"Attributes to Rename\".")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ALWAYS_ADD = new PropertyDescriptor.Builder()
            .name("Always Add")
            .description("If true an attribute will be added for each \"New Name\" regardless if the relevant "
                    + "field exists or not. This is ignored if \"Add null Fields\" is false.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(NONE)
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
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(ATTRIBUTE_PREFIX);
        descriptors.add(ADD_NULL_FIELDS);
        descriptors.add(ADD_FRAG);
        descriptors.add(NAME_DELIM);
        descriptors.add(FIELD_NAMES);
        descriptors.add(NEW_NAMES);
        descriptors.add(ALWAYS_ADD);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
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

        // Get property values.
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String attPrefix = context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        final boolean addNullFields = context.getProperty(ADD_NULL_FIELDS).asBoolean();
        final String addFragAtts = context.getProperty(ADD_FRAG).getValue();
        //noinspection Annotator
        final String delim = "\\" + context.getProperty(NAME_DELIM).evaluateAttributeExpressions(flowFile).getValue();
        final boolean alwaysAdd = context.getProperty(ALWAYS_ADD).asBoolean();

        // Get the JSON from the flowfile.
        final AtomicReference<String> content;
        if (attName != null && attName.length() > 0) {
            content = new AtomicReference<>();
            content.set(flowFile.getAttribute(attName));
        } else {
            content = FlowUtils.readContent(session, flowFile);
        }

        final Object jsonObject = new JsonSlurper().setType(LAX).parseText(content.get());
        boolean isArray = false;
        ArrayList<Map<String, Object>> jsonRecords = new ArrayList<>();

        try {
            if (jsonObject instanceof List) {
                List jsonList = (List) jsonObject;
                for (Object jsonItem : jsonList) {
                    if (jsonItem instanceof Map) {
                        jsonRecords.add((Map<String, Object>)jsonItem);
                    }
                }
                isArray = true;
            }
            else if (jsonObject instanceof Map) {
                jsonRecords.add((Map<String, Object>)jsonObject);
            }
            else {
                flowFile = session.putAttribute(flowFile, "json.failure.reason", "The supplied JSON is not an array or "
                        + "an object.");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }
        catch (Exception e) {
            flowFile = session.putAttribute(flowFile, "json.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final int fragCount = jsonRecords.size();
        int fragIndex = 0;
        final String fragID = UUID.randomUUID().toString();

        String[] renameList = new String[0];
        final String renameCSV = context.getProperty(FIELD_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        if (renameCSV != null && renameCSV.length() > 0) renameList = renameCSV.split(delim);

        String[] newNameList = new String[0];
        final String newNameCSV = context.getProperty(NEW_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        if (newNameCSV != null && newNameCSV.length() > 0) newNameList = newNameCSV.split(delim);

        // Iterate the records.
        for (Map<String, Object> record : jsonRecords) {
            FlowFile newFlowFile = session.create(flowFile);
            fragIndex++;
            if (alwaysAdd && addNullFields) {
                for (String name : newNameList) {
                    if (attPrefix != null && attPrefix.length() > 0) name = attPrefix + name;
                    newFlowFile = session.putAttribute(newFlowFile, name, "");
                }
            }
            // Iterate the field elements.
            for (String key : record.keySet()) {
                Object fieldValue = record.get(key);
                if (fieldValue instanceof Map || fieldValue instanceof List) continue;
                // Check if we need to rename the field
                for (int renameIndex = 0; renameIndex < renameList.length; renameIndex++) {
                    String value = renameList[renameIndex];
                    if (Objects.equals(value, key)) {
                        if (renameIndex < newNameList.length) {
                            key = newNameList[renameIndex];
                        }
                        break;
                    }
                }
                // Add an attribute to the flowfile for the field.
                if (attPrefix != null && attPrefix.length() > 0) key = attPrefix + key;
                if (fieldValue == null && addNullFields) fieldValue = "";
                if (fieldValue != null) {
                    newFlowFile = session.putAttribute(newFlowFile, key, fieldValue.toString());
                }
            }

            // Add fragment attributes.
            if (addFragAtts == "Always" || (addFragAtts == "JSON Arrays Only") && isArray) {
                newFlowFile = session.putAttribute(newFlowFile, "fragment.identifier", fragID);
                newFlowFile = session.putAttribute(newFlowFile, "fragment.index", Integer.toString(fragIndex));
                newFlowFile = session.putAttribute(newFlowFile, "fragment.count", Integer.toString(fragCount));
                newFlowFile = session.putAttribute(newFlowFile, "fragment.size", "0");
            }
            // Transfer the new FlowFile.
            session.transfer(newFlowFile, REL_SUCCESS);
        }

        // Transfer the original FlowFile.
        session.transfer(flowFile, REL_ORIGINAL);
    }
}