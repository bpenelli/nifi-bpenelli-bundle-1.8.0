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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"update", "flowfile", "attribute", "content", "expression", "recurse", "bpenelli"})
@CapabilityDescription("Add or update FlowFile attributes and/or content with options for ignoring and recursing expression language.")
@SeeAlso()
@ReadsAttributes({@ReadsAttribute(attribute = "")})
@WritesAttributes({
        @WritesAttribute(attribute = "update.failure.reason", description = "The reason the FlowFile was sent to failure relationship."),
})
public class UpdateFlowFile extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an exception")
            .build();

    public static final PropertyDescriptor NEW_CONTENT = new PropertyDescriptor.Builder()
            .name("New Content")
            .description("New FlowFile content. If empty, the contents will be left as is.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EVAL_CONTENT = new PropertyDescriptor.Builder()
            .name("Evaluate Content Expression Language")
            .description("If true, expression language will be evaluated on the given 'New Content'. Default false.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EVAL_ATTS = new PropertyDescriptor.Builder()
            .name("Evaluate Attribute Expression Language")
            .description("If true, expression language will be evaluated on the given attributes. Default false.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EVAL_RECUR = new PropertyDescriptor.Builder()
            .name("Evaluate Recursively")
            .description("If true, expression language will be evaluated recursively to support dynamically generated expressions. Default false.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues("true", "false")
            .defaultValue("false")
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
        descriptors.add(NEW_CONTENT);
        descriptors.add(EVAL_CONTENT);
        descriptors.add(EVAL_ATTS);
        descriptors.add(EVAL_RECUR);
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
                .expressionLanguageSupported(true)
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

        final boolean evalContent = context.getProperty(EVAL_CONTENT).asBoolean();
        final boolean evalAttributes = context.getProperty(EVAL_ATTS).asBoolean();
        final boolean evalRecur = context.getProperty(EVAL_RECUR).asBoolean();
        String newContent = context.getProperty(NEW_CONTENT).getValue();

        try {

            if (newContent != null && !newContent.isEmpty()) {
                if (evalContent) {
                    String lastContent = newContent;
                    newContent = FlowUtils.evaluateExpression(context, flowFile, newContent);
                    if (evalRecur) {
                        while (!newContent.equals(lastContent)) {
                            lastContent = newContent;
                            newContent = FlowUtils.evaluateExpression(context, flowFile, newContent);
                        }
                    }
                }
                // Add or update the content.
                flowFile = FlowUtils.writeContent(session, flowFile, newContent);
            }

            // Find the dynamic properties.
            for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (descriptor.isDynamic()) {
                    String value = context.getProperty(descriptor).getValue();
                    if (evalAttributes) {
                        String lastValue = value;
                        value = FlowUtils.evaluateExpression(context, flowFile, value);
                        if (evalRecur) {
                            while (!value.equals(lastValue)) {
                                lastValue = value;
                                value = FlowUtils.evaluateExpression(context, flowFile, value);
                            }
                        }
                    }
                    // Add or update the attribute.
                    flowFile = session.putAttribute(flowFile, descriptor.getName(), value);
                }
            }

            // Transfer the FlowFile to success and commit the session.
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.toString();
            flowFile = session.putAttribute(flowFile, "update.failure.reason", msg);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to process {} due to {}", new Object[]{flowFile, e});
        }
    }
}