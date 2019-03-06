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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;
import org.w3c.dom.Element;
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
@Tags({"evaluate", "expression language", "xpath", "xml", "bpenelli"})
@CapabilityDescription("Extracts values from XML, using XPath, to FlowFile attributes. The source XML can come from the "
        + "FlowFile's content, a cache entry, or FlowFile attribute, depending on configuration. XPath expressions "
        + "can contain expression language.")
@WritesAttributes({@WritesAttribute(attribute = "eval.failure.reason", description = "The reason the FlowFile was sent to failue relationship.")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class EvaluateXML extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be processed")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute Name")
            .description("The name of the attribute containing source XML. If left empty the FlowFile's contents will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor CACHE_KEY = new PropertyDescriptor.Builder()
            .name("Cache Key")
            .description("The key to a cached entry containing XML.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
            .name("Distributed Map Cache Service")
            .description("The Controller Service used to access cached XML.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .addValidator(Validator.VALID)
            .build();

    protected List<PropertyDescriptor> descriptors;
    protected Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ATTRIBUTE_NAME);
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
     * getSupportedDynamicPropertyDescriptor
     **************************************************************/
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .required(false)
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
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        // Get the XML
        final AtomicReference<String> content;
        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);
        final String cacheKey = context.getProperty(CACHE_KEY).evaluateAttributeExpressions(flowFile).getValue();


        // Get the XML from an attribute, or cache entry, or the FlowFile's contents, whichever is found first.
        if (attName != null && attName.length() > 0) {
            content = new AtomicReference<>();
            content.set(flowFile.getAttribute(attName));
        } else if (cacheService != null && cacheKey != null && cacheKey.length() > 0) {
            content = new AtomicReference<>();
            try {
                content.set(cacheService.get(cacheKey, FlowUtils.stringSerializer, FlowUtils.stringDeserializer));
            } catch (IOException e) {
                flowFile = session.putAttribute(flowFile, "eval.failure.reason", e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        } else {
            content = FlowUtils.readContent(session, flowFile);
        }

        final XPath xpath = XPathFactory.newInstance().newXPath();
        final DocumentBuilder builder;

        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            flowFile = session.putAttribute(flowFile, "eval.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final ByteArrayInputStream xmlInputStream = new ByteArrayInputStream(content.get().getBytes());
        final Element records;

        try {
            records = builder.parse(xmlInputStream).getDocumentElement();
        } catch (SAXException | IOException e) {
            flowFile = session.putAttribute(flowFile, "eval.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Find dynamic properties and evaluate the XPath.
        for (PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (propDesc.isDynamic()) {
                PropertyValue propVal = context.getProperty(propDesc);
                final String xpathQuery = propVal.evaluateAttributeExpressions(flowFile).getValue();
                final Object text;
                try {
                    text = xpath.evaluate(xpathQuery, records, XPathConstants.STRING);
                } catch (XPathExpressionException e) {
                    flowFile = session.putAttribute(flowFile, "eval.failure.reason", e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
                // Save the results to an attribute.
                flowFile = session.putAttribute(flowFile, propDesc.getName(), text.toString());
            }
        }

        // Transfer the FlowFile.
        session.transfer(flowFile, REL_SUCCESS);
    }
}
