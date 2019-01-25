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

import org.apache.hadoop.hbase.client.Connection;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.validate.ConfigFilesValidator;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bpenelli.utils.FlowUtils;
import org.apache.nifi.processors.bpenelli.utils.HBaseUtils;

import java.util.*;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@Tags({"truncate", "hbase", "table", "bpenelli"})
@CapabilityDescription("Truncates one or more HBase tables.")
@WritesAttributes({
        @WritesAttribute(attribute = "truncate.failure.reason", description = "The reason the FlowFile was sent to failure relationship.")
})
public class TruncateHBase extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile with an exception")
            .build();

    public static final PropertyDescriptor TABLE_NAMES = new PropertyDescriptor.Builder()
            .name("Table Names")
            .description("A comma delimited list of HBase table names to truncate.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files," +
                    " such as hbase-site.xml and core-site.xml, including full paths to the files.")
            .addValidator(new ConfigFilesValidator())
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for HBase. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_CLIENT_PORT = new PropertyDescriptor.Builder()
            .name("ZooKeeper Client Port")
            .description("The port on which ZooKeeper is accepting client connections. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_ZNODE_PARENT = new PropertyDescriptor.Builder()
            .name("ZooKeeper ZNode Parent")
            .description("The ZooKeeper ZNode Parent value for HBase (example: /hbase). Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor HBASE_CLIENT_RETRIES = new PropertyDescriptor.Builder()
            .name("HBase Client Retries")
            .description("The number of times the HBase client will retry connecting.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    /**************************************************************
     * init
     **************************************************************/
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TABLE_NAMES);
        descriptors.add(HADOOP_CONF_FILES);
        descriptors.add(ZOOKEEPER_QUORUM);
        descriptors.add(ZOOKEEPER_CLIENT_PORT);
        descriptors.add(ZOOKEEPER_ZNODE_PARENT);
        descriptors.add(HBASE_CLIENT_RETRIES);
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

        // Get property values.
        final String tableNames = context.getProperty(TABLE_NAMES).evaluateAttributeExpressions(flowFile).getValue();
        final String configFiles = context.getProperty(HADOOP_CONF_FILES).evaluateAttributeExpressions().getValue();
        final String zkQuorum = context.getProperty(ZOOKEEPER_QUORUM).evaluateAttributeExpressions(flowFile).getValue();
        final String zkPort = context.getProperty(ZOOKEEPER_CLIENT_PORT).evaluateAttributeExpressions(flowFile).getValue();
        final String zkZNodeParent = context.getProperty(ZOOKEEPER_ZNODE_PARENT).evaluateAttributeExpressions(flowFile).getValue();
        final String hbaseClientRetries = context.getProperty(HBASE_CLIENT_RETRIES).evaluateAttributeExpressions(flowFile).getValue();

        try {
            if (!context.getProperty(HADOOP_CONF_FILES).isSet()) {
                if (!context.getProperty(ZOOKEEPER_QUORUM).isSet()
                        || !context.getProperty(ZOOKEEPER_CLIENT_PORT).isSet()
                        || !context.getProperty(ZOOKEEPER_ZNODE_PARENT).isSet()
                ) {
                    throw new java.util.InputMismatchException("At a minimum, either Hadoop configuration files or " +
                            "all ZooKeeper properties are required.");
                }
            }
            final Connection connection = HBaseUtils.createConnection(configFiles, zkQuorum, zkPort, zkZNodeParent,
                    hbaseClientRetries, FlowUtils.getDynamicPropertyMap(context, flowFile));
            for (String tableName : tableNames.split(",")) {
                HBaseUtils.truncate(connection, tableName);
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            session.putAttribute(flowFile, "truncate.failure.reason", e.getMessage());
            getLogger().error("Unable to process {} due to {}", new Object[]{flowFile, e});
        }
    }
}