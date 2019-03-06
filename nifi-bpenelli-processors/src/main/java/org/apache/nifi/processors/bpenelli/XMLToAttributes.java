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

import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"xml", "attributes", "bpenelli"})
@CapabilityDescription("Extracts XML elements to FlowFile attributes. The XML can come from the "
        + "FlowFile's content, or a FlowFile attribute. If \"Parse Type\" is table, then a new "
        + "FlowFile will be generated for each record element. If \"Parse Type\" is record, then "
        + "only one FlowFile will be generated. You can also choose to use different names for "
        + "the attributes when extracted.")
@SeeAlso()
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class XMLToAttributes extends AbstractProcessor {

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

    public static final PropertyDescriptor XML_ROOT = new PropertyDescriptor.Builder()
            .name("XML Root Path")
            .description("The path to the root node containing the elements to extract.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor PARSE_TYPE = new PropertyDescriptor.Builder()
            .name("Parse Type")
            .description("table - Treats root node elements as records and outputs a FlowFile for each. record - Treats root node elements as fields and extracts to the existing FlowFile.")
            .required(true)
            .allowableValues("table", "record")
            .defaultValue("table")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute Name")
            .description("The name of the attribute containing source XML. If left empty the FlowFile's contents will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("Attribute Prefix")
            .description("A prefix to use on all the resulting attribute names.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor NAME_DELIM = new PropertyDescriptor.Builder()
            .name("Name Delimiter")
            .description("Delimiter used to separate names in the \"Attributes to Rename\" and \"New Names\" properties. Defaults to comma.")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ELEM_NAMES = new PropertyDescriptor.Builder()
            .name("Attributes to Rename")
            .description("Delimited list of element names to change when creating the FlowFile attributes.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor NEW_NAMES = new PropertyDescriptor.Builder()
            .name("New Names")
            .description("Delimited list of new names to apply to the \"Attributes to Rename\".")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ALWAYS_ADD = new PropertyDescriptor.Builder()
            .name("Always Add")
            .description("If true an attribute will be added for each \"New Name\" regardless if the relevant field-level element exists or not.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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
        descriptors.add(XML_ROOT);
        descriptors.add(PARSE_TYPE);
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(ATTRIBUTE_PREFIX);
        descriptors.add(NAME_DELIM);
        descriptors.add(ELEM_NAMES);
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

        // Get the XML
        final AtomicReference<String> content;
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String attPrefix = context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        if (attName != null && attName.length() > 0) {
            content = new AtomicReference<>();
            content.set(flowFile.getAttribute(attName));
        } else {
            content = FlowUtils.readContent(session, flowFile);
        }

        // Extract the XML
        final StringBuilder rootPath = new StringBuilder(context.getProperty(XML_ROOT).evaluateAttributeExpressions(flowFile).getValue());
        final String parseType = context.getProperty(PARSE_TYPE).getValue();
        if (Objects.equals(parseType, "table")) {
            rootPath.append("/child::*");
        }
        //noinspection Annotator
        final String delim = "\\" + context.getProperty(NAME_DELIM).evaluateAttributeExpressions(flowFile).getValue();
        final boolean alwaysAdd = context.getProperty(ALWAYS_ADD).asBoolean();
        final XPath xpath = XPathFactory.newInstance().newXPath();
        DocumentBuilder builder;

        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to extract XML to attributes for {} due to {}", new Object[]{flowFile, e});
            return;
        }

        final ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(content.get().getBytes());

        Element doc;
        try {
            doc = builder.parse(xmlInputStream).getDocumentElement();
        } catch (SAXException | IOException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to extract XML to attributes for {} due to {}", new Object[]{flowFile, e});
            return;
        }

        NodeList records;
        try {
            records = (NodeList) xpath.evaluate(rootPath.toString(), doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to extract XML to attributes for {} due to {}", new Object[]{flowFile, e});
            return;
        }

        final int fragCount = records.getLength();
        int fragIndex = 0;
        final String fragID = UUID.randomUUID().toString();

        String[] renameList = new String[0];
        final String renameCSV = context.getProperty(ELEM_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        if (renameCSV != null && renameCSV.length() > 0) renameList = renameCSV.split(delim);

        String[] newNameList = new String[0];
        final String newNameCSV = context.getProperty(NEW_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        if (newNameCSV != null && newNameCSV.length() > 0) newNameList = newNameCSV.split(delim);

        // Iterate the record elements.
        for (int i = 0; i < fragCount; i++) {
            final Node record = records.item(i);
            FlowFile newFlowFile = session.create(flowFile);
            NodeList fields;
            try {
                fields = (NodeList) xpath.evaluate("./child::*", record, XPathConstants.NODESET);
            } catch (XPathExpressionException e) {
                getLogger().error("Unable to extract XML to attributes for {} due to {}", new Object[]{flowFile, e});
                session.rollback();
                return;
            }
            final int fieldCount = fields.getLength();
            fragIndex++;
            if (alwaysAdd) {
                for (String name : newNameList) {
                    newFlowFile = session.putAttribute(newFlowFile, name, "");
                }
            }
            // Iterate the field elements.
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                Node field = fields.item(fieldIndex);
                String fieldName = field.getNodeName();
                final String fieldValue = field.getTextContent();
                // Check if we need to rename the field
                for (int renameIndex = 0; renameIndex < renameList.length; renameIndex++) {
                    String value = renameList[renameIndex];
                    if (Objects.equals(value, fieldName)) {
                        if (renameIndex < newNameList.length) {
                            fieldName = newNameList[renameIndex];
                        }
                        break;
                    }
                }
                if (attPrefix != null && attPrefix.length() > 0) fieldName = attPrefix + fieldName;
                newFlowFile = session.putAttribute(newFlowFile, fieldName, fieldValue);
            }

            if (Objects.equals(parseType, "table")) {
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
