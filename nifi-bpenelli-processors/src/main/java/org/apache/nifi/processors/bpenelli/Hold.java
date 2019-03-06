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

import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
@Tags({"hold, release, topic, key, cache, flowfile, bpenelli"})
@CapabilityDescription("Allows one FlowFile through for a given topic and key, and holds up the remaining "
        + "FlowFiles for the same topic and key, until the first one is released by a companion Release processor.")
@DynamicRelationship(name = "duplicate", description = "FlowFiles are sent to this relationship when a Duplicate Value "
        + "is supplied and there is a FlowFile with the same Topic, Key, and Duplicate Value already in progress.")
@SeeAlso(classNames = {"org.bpenelli.nifi.processors.Release"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class Hold extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_BUSY = new Relationship.Builder()
            .name("busy")
            .description("Any FlowFile whose topic and key haven't been released yet")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an exception")
            .build();
    public static final PropertyDescriptor KEY_TOPIC = new PropertyDescriptor.Builder()
            .name("Topic")
            .description("The hold topic name.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Key")
            .description("The hold key.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor DUP_VALUE = new PropertyDescriptor.Builder()
            .name("Duplicate Value")
            .description("If supplied, and if the Topic, Key, and this value match one that's already in progress, "
                    + "the FlowFile will be sent to the duplicate relationship.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor CACHE_SVC = new PropertyDescriptor.Builder()
            .name("Distributed Map Cache Service")
            .description("The Controller Service providing map cache services.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .addValidator(Validator.VALID)
            .build();
    public static Relationship REL_DUP = null;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private Set<Relationship> dynamicRelationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEY_TOPIC);
        descriptors.add(KEY_VALUE);
        descriptors.add(DUP_VALUE);
        descriptors.add(CACHE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_BUSY);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
        this.dynamicRelationships = Collections.unmodifiableSet(new HashSet<>());
    }

    /**************************************************************
     * onPropertyModified
     **************************************************************/
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(DUP_VALUE)) {
            if (newValue != null && newValue.length() > 0) {
                final Set<Relationship> dynamicRelationships = new HashSet<>();
                Hold.REL_DUP = new Relationship.Builder()
                        .name("duplicate")
                        .description("Any FlowFile which is determined to be a duplicate")
                        .build();
                dynamicRelationships.add(REL_DUP);
                this.dynamicRelationships = Collections.unmodifiableSet(dynamicRelationships);
            } else {
                this.dynamicRelationships = Collections.unmodifiableSet(new HashSet<>());
            }
        }
    }

    /**************************************************************
     * getRelationships
     **************************************************************/
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> allRelationships = new HashSet<>();
        allRelationships.addAll(this.relationships);
        allRelationships.addAll(this.dynamicRelationships);
        return Collections.unmodifiableSet(allRelationships);
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

        final String keyTopic = context.getProperty(KEY_TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String keyValue = context.getProperty(KEY_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String holdKey = keyTopic + "." + keyValue;
        final String dupValue = context.getProperty(DUP_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final boolean checkDup = dupValue != null && dupValue.length() > 0;
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);

        try {
            if (checkDup && dupValue.equals(cacheService.get(holdKey, FlowUtils.stringSerializer, FlowUtils.stringDeserializer))) {
                session.transfer(flowFile, REL_DUP);
            } else if (cacheService.containsKey(holdKey, FlowUtils.stringSerializer)) {
                session.transfer(flowFile, REL_BUSY);
            } else {
                if (checkDup) {
                    if (!cacheService.putIfAbsent(holdKey, dupValue, FlowUtils.stringSerializer, FlowUtils.stringSerializer)) {
                        session.rollback();
                        return;
                    }
                } else {
                    if (!cacheService.putIfAbsent(holdKey, "!--holding--!", FlowUtils.stringSerializer, FlowUtils.stringSerializer)) {
                        session.rollback();
                        return;
                    }
                }
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to Hold topic and key for {} due to {}", new Object[]{flowFile, e});
        } finally {
            session.commit();
        }
    }
}
