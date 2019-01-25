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
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"xml", "validate", "xsd", "bpenelli"})
@CapabilityDescription("Validates a FlowFile's XML against one or more XSDs.")
@WritesAttributes({
        @WritesAttribute(attribute = "xml.failure.reason", description = "The reason the FlowFile was sent to failue relationship."),
        @WritesAttribute(attribute = "xml.invalid.reason", description = "The reason the FlowFile was sent to invalid relationship.")
})
@SeeAlso()
public class XMLValidator extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that pass validation")
            .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("All FlowFiles that fail validation")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cause exceptions")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("XML Attribute")
            .description("The name of the attribute containing source XML. If left empty the FlowFile's contents will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor XSD_PATH = new PropertyDescriptor.Builder()
            .name("XSD Path")
            .description("The path to one or more XSD files to validate against. If more than one, separate with a comma.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor XSD_TEXT = new PropertyDescriptor.Builder()
            .name("XSD Text")
            .description("The text of an XSD to validate against. If supplied, this will override XSD Path.")
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
        descriptors.add(XSD_PATH);
        descriptors.add(XSD_TEXT);
        descriptors.add(ATTRIBUTE_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_INVALID);
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
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String xsdPath = context.getProperty(XSD_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String xsdText = context.getProperty(XSD_TEXT).evaluateAttributeExpressions(flowFile).getValue();

        final String content;
        final List<String> xsdPathList = new ArrayList<>();
        final List<Source> xsdSources = new ArrayList<>();

        if (xsdPath != null && xsdPath.length() > 0) {
            xsdPathList.addAll(Arrays.asList(xsdPath.split(",")));
        }

        // Get the XML content.
        if (attName != null && attName.length() > 0) {
            content = flowFile.getAttribute(attName);
        } else {
            content = FlowUtils.readContent(session, flowFile).get();
        }

        // Get the XSD source streams.
        if (xsdPathList.size() > 0) {
            for (String path : xsdPathList) {
                File xsdFile = new File(path);
                try {
                    xsdSources.add(new StreamSource(new FileInputStream(xsdFile)));
                } catch (FileNotFoundException e) {
                    flowFile = session.putAttribute(flowFile, "xml.failure.reason", e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }
        } else {
            xsdSources.add(new StreamSource(new StringReader(xsdText)));
        }

        // Validate
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = factory.newSchema(xsdSources.toArray(new Source[xsdPathList.size()]));
            javax.xml.validation.Validator validator = schema.newValidator();
            validator.validate(new StreamSource(new StringReader(content)));
        } catch (SAXException e) {
            flowFile = session.putAttribute(flowFile, "xml.invalid.reason", e.getMessage());
            session.transfer(flowFile, REL_INVALID);
            return;
        } catch (Exception e) {
            flowFile = session.putAttribute(flowFile, "xml.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}