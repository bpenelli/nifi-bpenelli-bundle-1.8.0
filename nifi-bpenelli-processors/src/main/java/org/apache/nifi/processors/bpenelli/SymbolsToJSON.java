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

import groovy.json.JsonBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;

import java.util.*;
import java.util.regex.Pattern;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"attributes", "to", "json", "regex", "match", "properties", "bpenelli"})
@CapabilityDescription("Generates a JSON representation of specified symbols (attributes and dynamic properties).")
@SeeAlso()
public class SymbolsToJSON extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final PropertyDescriptor ATT_PREFIX = new PropertyDescriptor.Builder()
            .name("Matching Prefix")
            .description("All attributes with this prefix will be included.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor STRIP_PREFIX = new PropertyDescriptor.Builder()
            .name("Strip Prefix")
            .description("Determines if the Matching Prefix will be stripped from the final name.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor FILTER_REGEX = new PropertyDescriptor.Builder()
            .name("RegEx")
            .description("All attributes that match this regular expression will be included.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATT_LIST = new PropertyDescriptor.Builder()
            .name("Attribute List")
            .description("A delimited list of attributes to include.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATT_DELIM = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .description("Delimiter used to separate attribute names in the Attributes List property. Defaults to comma.")
            .required(true)
            .defaultValue(",")
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
        descriptors.add(ATT_PREFIX);
        descriptors.add(STRIP_PREFIX);
        descriptors.add(FILTER_REGEX);
        descriptors.add(ATT_LIST);
        descriptors.add(ATT_DELIM);
        this.descriptors = Collections.unmodifiableList(descriptors);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**************************************************************
     * getSupportedDynamicPropertyDescriptor
     **************************************************************/
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {

        PropertyDescriptor.Builder propertyBuilder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true);

        return propertyBuilder.build();
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

        // Get Property Values
        final String attPrefix = context.getProperty(ATT_PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        final boolean excludePrefix = context.getProperty(STRIP_PREFIX).asBoolean();
        final String regex = context.getProperty(FILTER_REGEX).evaluateAttributeExpressions(flowFile).getValue();
        final String delim = context.getProperty(ATT_DELIM).evaluateAttributeExpressions(flowFile).getValue();
        final String attNames = context.getProperty(ATT_LIST).evaluateAttributeExpressions(flowFile).getValue();

        final Map<String, String> valueMap = new TreeMap<>();

        // Get attributes with the specified prefix.
        if (attPrefix != null && attPrefix.length() > 0) {
            for (final String name : flowFile.getAttributes().keySet()) {
                if (name.startsWith(attPrefix)) {
                    if (excludePrefix) {
                        valueMap.put(name.replaceFirst(attPrefix, ""), flowFile.getAttribute(name));
                    } else {
                        valueMap.put(name, flowFile.getAttribute(name));
                    }
                }
            }
        }

        // Get attributes that match the specified regular expression.
        if (regex != null && regex.length() > 0) {
            final Pattern filterPattern = Pattern.compile(regex);
            for (final String name : flowFile.getAttributes().keySet()) {
                if (filterPattern.matcher(name).matches()) {
                    valueMap.put(name, flowFile.getAttribute(name));
                }
            }
        }

        // Get attributes in the specified list.
        if (attNames != null && attNames.length() > 0) {
            for (final String name : attNames.split(delim)) {
                valueMap.put(name, flowFile.getAttribute(name));
            }
        }

        // Get dynamic properties.
        for (PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (propDesc.isDynamic()) {
                PropertyValue propVal = context.getProperty(propDesc);
                valueMap.put(propDesc.getName(), propVal.evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        // Build a JSON object from the results and put it in the FlowFile's content.
        final JsonBuilder builder = new JsonBuilder();
        builder.call(valueMap);
        flowFile = FlowUtils.writeContent(session, flowFile, builder.toString());

        // Transfer the FlowFile to success.
        session.transfer(flowFile, REL_SUCCESS);
    }
}