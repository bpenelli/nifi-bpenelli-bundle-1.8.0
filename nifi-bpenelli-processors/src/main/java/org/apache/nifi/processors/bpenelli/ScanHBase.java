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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.HBaseResults;
import org.apache.nifi.processors.bpenelli.utils.HBaseUtils;

import java.io.IOException;
import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"get", "hbase", "scan", "filter", "query", "bpenelli"})
@CapabilityDescription("Runs a filtered HBase scan.")
@WritesAttributes({
        @WritesAttribute(attribute = "scan.failure.reason", description = "The reason the FlowFile was sent to failure relationship.")
})
public class ScanHBase extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an IO exception")
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase table to use to maintain the sequence.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ROW_KEY_NAME = new PropertyDescriptor.Builder()
            .name("Row Key Name")
            .description("The name to assign the row key value to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("row_key")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILTER_EXP = new PropertyDescriptor.Builder()
            .name("Filter Expression")
            .description("An HBase filter expression.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ATTR_FORMAT = new PropertyDescriptor.Builder()
            .name("Attribute Format")
            .description("The format to use for the resulting attribute names.")
            .required(true)
            .allowableValues(HBaseResults.FMT_TBL_FAM_QUAL, HBaseResults.FMT_TBL_QUAL, HBaseResults.FMT_FAM_QUAL, HBaseResults.FMT_QUAL)
            .defaultValue(HBaseResults.FMT_QUAL)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the HBase Client Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TABLE_NAME);
        descriptors.add(ROW_KEY_NAME);
        descriptors.add(FILTER_EXP);
        descriptors.add(ATTR_FORMAT);
        descriptors.add(HBASE_CLIENT_SERVICE);
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
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String rowKeyName = context.getProperty(ROW_KEY_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String filterExpression = context.getProperty(FILTER_EXP).evaluateAttributeExpressions(flowFile).getValue();
        final String attrFormat = context.getProperty(ATTR_FORMAT).getValue();
        final HBaseClientService hbaseService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        try {
            // Scan the HBase table.
            final HBaseResults scanResults;
            scanResults = HBaseUtils.scan(hbaseService, tableName, filterExpression);
            scanResults.rowKeyName = rowKeyName;
            scanResults.emitFormat = attrFormat;
            // Emit a new flow file for each result row.
            scanResults.emitFlowFiles(session, flowFile, REL_SUCCESS);
            session.remove(flowFile);
        } catch (IOException e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.toString();
            flowFile = session.putAttribute(flowFile, "scan.failure.reason", msg);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Unable to process {} due to {}", new Object[]{flowFile, e});
        }
    }
}
