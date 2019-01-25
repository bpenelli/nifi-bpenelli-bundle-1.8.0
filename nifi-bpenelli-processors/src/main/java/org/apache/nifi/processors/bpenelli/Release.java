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
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.io.IOException;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"hold, release, topic, key, cache, flowfile, bpenelli"})
@CapabilityDescription("Releases the Hold on a given topic and key.")
@SeeAlso(classNames = {"org.bpenelli.nifi.processors.Hold"})
public class Release extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile whose Hold topic and key is successfully released")
            .build();

    public static final Relationship REL_MISSING = new Relationship.Builder()
            .name("missing")
            .description("Any FlowFile whose Hold topic and key were not found in cache")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an IO exception")
            .build();

    public static final PropertyDescriptor KEY_TOPIC = new PropertyDescriptor.Builder()
            .name("Topic")
            .description("The Hold topic name.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor KEY_VALUE = new PropertyDescriptor.Builder()
            .name("Key")
            .description("The Hold key.")
            .required(true)
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

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEY_TOPIC);
        descriptors.add(KEY_VALUE);
        descriptors.add(CACHE_SVC);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_MISSING);
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

        final String keyTopic = context.getProperty(KEY_TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String keyValue = context.getProperty(KEY_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String holdKey = keyTopic + "." + keyValue;
        final DistributedMapCacheClient cacheService = context.getProperty(CACHE_SVC).asControllerService(DistributedMapCacheClient.class);

        try {
            if (cacheService.containsKey(holdKey, FlowUtils.stringSerializer)) {
                cacheService.remove(holdKey, FlowUtils.stringSerializer);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_MISSING);
            }
        } catch (IOException e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to Release topic and key for {} due to {}", new Object[]{flowFile, e});
        } finally {
            session.commit();
        }

    }
}