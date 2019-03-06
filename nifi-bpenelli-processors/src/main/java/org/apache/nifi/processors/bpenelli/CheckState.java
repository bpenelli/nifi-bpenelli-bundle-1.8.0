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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"check", "update", "state", "cache", "compare", "route", "bpenelli"})
@CapabilityDescription("Routes a FlowFile and updates state based on a supplied compare value relative to a state value.")
@SeeAlso()
@ReadsAttributes({@ReadsAttribute(attribute = "")})
@WritesAttributes({
        @WritesAttribute(attribute = "check.failure.reason", description = "The reason the FlowFile was sent to failure relationship."),
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class CheckState extends AbstractProcessor {

    public static final Relationship REL_LESS_THAN = new Relationship.Builder()
            .name("less")
            .description("Any FlowFile who's compare value is less than state.")
            .build();
    public static final Relationship REL_GREATER_THAN = new Relationship.Builder()
            .name("greater")
            .description("Any FlowFile who's compare value is greater than state.")
            .build();
    public static final Relationship REL_EQUAL_TO = new Relationship.Builder()
            .name("equal")
            .description("Any FlowFile who's compare value is equal to state.")
            .build();
    public static final Relationship REL_EMPTY = new Relationship.Builder()
            .name("empty")
            .description("Any FlowFile with a missing or empty compare value.")
            .build();
    public static final Relationship REL_INITIAL = new Relationship.Builder()
            .name("initial")
            .description("Any FlowFile where a state key is initialized.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an exception.")
            .build();
    public static final PropertyDescriptor STATE_KEY = new PropertyDescriptor.Builder()
            .name("State Key")
            .description("The key containing the state value to compare with.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor COMPARE_VAL = new PropertyDescriptor.Builder()
            .name("Compare Value")
            .description("The value to compare with state. If left empty, the FlowFile's content will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
            .name("Distributed Map Cache Service")
            .description("The map cache service providing access to state.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor PREV_STATE_ATTR = new PropertyDescriptor.Builder()
            .name("Previous State Attribute Name")
            .description("The name of an attribute to output the previous state value to.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    static final String LESS_THAN = "Less Than";
    static final String GREATER_THAN = "Greater Than";
    static final String EQUAL_TO = "Equal To";
    static final String NOT_EQUAL_TO = "Not Equal To";
    static final String NEVER = "Never";
    public static final PropertyDescriptor REPLACE_WHEN = new PropertyDescriptor.Builder()
            .name("Replace When")
            .description("Replace state with compare value when compare value " +
                    "is one of the following relative to state.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(LESS_THAN, GREATER_THAN, EQUAL_TO, NOT_EQUAL_TO, NEVER)
            .defaultValue(GREATER_THAN)
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
        descriptors.add(STATE_KEY);
        descriptors.add(COMPARE_VAL);
        descriptors.add(REPLACE_WHEN);
        descriptors.add(PREV_STATE_ATTR);
        descriptors.add(CACHE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_LESS_THAN);
        relationships.add(REL_GREATER_THAN);
        relationships.add(REL_EQUAL_TO);
        relationships.add(REL_EMPTY);
        relationships.add(REL_INITIAL);
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

        final String stateKey = context.getProperty(STATE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        String compareValue = context.getProperty(COMPARE_VAL).evaluateAttributeExpressions(flowFile).getValue();
        String prevStateAttr = context.getProperty(PREV_STATE_ATTR).evaluateAttributeExpressions(flowFile).getValue();
        final String replaceWhen = context.getProperty(REPLACE_WHEN).getValue();
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);

        try {

            // Route to empty if no compare value provided.
            if (compareValue == null || compareValue.isEmpty()) {
                compareValue = FlowUtils.readContent(session, flowFile).toString();
                if (compareValue == null || compareValue.isEmpty()) {
                    session.transfer(flowFile, REL_EMPTY);
                    return;
                }
            }

            // Get state for key.
            final String stateValue = cacheService.get(stateKey, FlowUtils.stringSerializer, FlowUtils.stringDeserializer);

            // If no state exists for key then initialize it and route to initial.
            if (stateValue == null || stateValue.isEmpty()) {
                cacheService.put(stateKey, compareValue, FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                session.transfer(flowFile, REL_INITIAL);
                return;
            }

            // Add previous state value to the requested attribute.
            if (prevStateAttr != null && !prevStateAttr.isEmpty()) {
                flowFile = session.putAttribute(flowFile, prevStateAttr, stateValue);
            }

            // Make comparison.
            final int result = compareValue.compareTo(stateValue);

            // Update state and route the FlowFile based on the result of comparison.
            if (result < 0) {
                if (replaceWhen.equals(LESS_THAN) || replaceWhen.equals(NOT_EQUAL_TO)) {
                    cacheService.put(stateKey, compareValue, FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                }
                session.transfer(flowFile, REL_LESS_THAN);
            } else if (result == 0) {
                if (replaceWhen.equals(EQUAL_TO)) {
                    cacheService.put(stateKey, compareValue, FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                }
                session.transfer(flowFile, REL_EQUAL_TO);
            } else {
                if (replaceWhen.equals(GREATER_THAN) || replaceWhen.equals(NOT_EQUAL_TO)) {
                    cacheService.put(stateKey, compareValue, FlowUtils.stringSerializer, FlowUtils.stringSerializer);
                }
                session.transfer(flowFile, REL_GREATER_THAN);
            }

        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.toString();
            flowFile = session.putAttribute(flowFile, "check.failure.reason", msg);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to process {} due to {}", new Object[]{flowFile, e});
        }
    }
}