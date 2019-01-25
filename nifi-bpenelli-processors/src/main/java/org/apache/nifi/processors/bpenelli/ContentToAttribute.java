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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"content, flowfile, attribute, extract, bpenelli"})
@CapabilityDescription("Extracts the contents of a FlowFile into a named attribute, and optionally " +
        "executes expression language against the extracted content.")
@SeeAlso()
public class ContentToAttribute extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Attribute Name")
            .description("The name of the attribute to extract the content into.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EXP_LANG = new PropertyDescriptor.Builder()
            .name("Expression Language")
            .description("Expression language to apply to the extracted content. Use the given \"Attribute Name\" to represent the content in the expression.")
            .required(false)
            .expressionLanguageSupported(true)
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
        descriptors.add(EXP_LANG);
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
        return descriptors;
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
        if (flowFile == null) {
            return;
        }

        final String attName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        // Read content.
        AtomicReference<String> content = FlowUtils.readContent(session, flowFile);

        // Save content to the named attribute.
        flowFile = session.putAttribute(flowFile, attName, content.get());

        // Apply any supplied expression language to the extracted content.
        final String expLang = context.getProperty(EXP_LANG).getValue();
        if (expLang != null && !expLang.isEmpty()) {
            final PropertyValue newPropVal = context.newPropertyValue(expLang);
            content.set(newPropVal.evaluateAttributeExpressions(flowFile).getValue());
            flowFile = session.putAttribute(flowFile, attName, content.get());
        }

        // Transfer the FlowFile to success and commit the session.
        session.transfer(flowFile, REL_SUCCESS);
    }
}