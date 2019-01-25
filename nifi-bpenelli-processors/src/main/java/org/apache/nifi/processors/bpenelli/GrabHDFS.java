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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractHadoopProcessor;
import org.apache.nifi.processors.hadoop.CompressionType;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@SuppressWarnings({"WeakerAccess", "EmptyMethod", "unused"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "grab", "HDFS", "get", "fetch", "ingest", "source", "filesystem"})
@CapabilityDescription("Grab files from Hadoop Distributed File System (HDFS) into FlowFiles.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file that was read from HDFS."),
        @WritesAttribute(attribute = "path", description = "The path of the file's directory on HDFS."),
        @WritesAttribute(attribute = "hdfs.comms.failure.reason", description = "The reason the FlowFile was sent to comms.failue relationship."),
        @WritesAttribute(attribute = "hdfs.failure.reason", description = "The reason the FlowFile was sent to failue relationship.")
})
@Restricted("Provides operator the ability to grab any file that NiFi has access to in HDFS or the local filesystem.")
public class GrabHDFS extends AbstractHadoopProcessor {

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All files retrieved from HDFS are transferred to this relationship").build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Unmodified input FlowFiles will be transfered to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieved and trying again will likely not be helpful. "
                    + "This would occur, for instance, if the file is not found or if there is a permissions issue")
            .build();

    public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder().name("comms.failure")
            .description(
                    "FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieve due to a communications failure. "
                            + "This generally indicates that the Grab should be tried again.")
            .build();
    // properties
    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
            .name("Keep Source File")
            .description("Determines whether to delete the file from HDFS after it has been successfully grabbed.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name("File Filter Regex")
            .description("A Java Regular Expression for filtering Filenames; if a filter is supplied then only files whose names match that Regular "
                    + "Expression will be fetched, otherwise all files will be fetched")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor IGNORE_DOTTED_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Dotted Files")
            .description("If true, files whose names begin with a dot (\".\") will be ignored")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    // other members
    public static final int BUFFER_SIZE_DEFAULT = 4096;
    protected ProcessorConfiguration processorConfig;

    /**************************************************************
     * getRelationships
     **************************************************************/
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_COMMS_FAILURE);
        return relationships;
    }

    /**************************************************************
     * getSupportedPropertyDescriptors
     **************************************************************/
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(DIRECTORY);
        props.add(KEEP_SOURCE_FILE);
        props.add(FILE_FILTER_REGEX);
        props.add(IGNORE_DOTTED_FILES);
        props.add(COMPRESSION_CODEC);
        return props;
    }

    /**************************************************************
     * customValidate
     **************************************************************/
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        return new ArrayList<>(super.customValidate(context));
    }

    /**************************************************************
     * onTrigger
     **************************************************************/
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        processorConfig = new ProcessorConfiguration(context);

        final FileSystem hdfs = getFileSystem();
        final Path dir = new Path(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
        boolean dir_exist;

        try {
            dir_exist = hdfs.exists(dir);
        } catch (IOException e) {
            flowFile = session.putAttribute(flowFile, "hdfs.comms.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_COMMS_FAILURE);
            return;
        }

        if (!dir_exist) {
            flowFile = session.putAttribute(flowFile, "hdfs.comms.failure.reason", "dir_does_not_exist");
            session.transfer(flowFile, REL_COMMS_FAILURE);
            return;
        }

        final FileSystem fs = getFileSystem();

        // Select files.
        Set<Path> files;
        try {
            files = selectFiles(fs, dir, null);
        } catch (IOException | InterruptedException e) {
            flowFile = session.putAttribute(flowFile, "hdfs.comms.failure.reason", e.getMessage());
            session.transfer(flowFile, REL_COMMS_FAILURE);
            return;
        }

        //  Grab files.
        if (!grabFiles(files, context, session, dir, flowFile)) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Route the original FlowFile.
        session.transfer(flowFile, REL_ORIGINAL);
        session.commit();
    }

    /**************************************************************
     * grabFiles
     **************************************************************/
    @SuppressWarnings("deprecation")
    protected boolean grabFiles(final Set<Path> files, final ProcessContext context,
                                final ProcessSession session, final Path rootDir, FlowFile parentFlowFile) {

        // Grab the batch of files.
        InputStream stream = null;
        CompressionCodec codec = null;
        Configuration conf = getConfiguration();
        FileSystem hdfs = getFileSystem();
        final boolean keepSourceFiles = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final CompressionType compressionType = CompressionType
                .valueOf(context.getProperty(COMPRESSION_CODEC).toString());
        final boolean inferCompressionCodec = compressionType == CompressionType.AUTOMATIC;

        if (inferCompressionCodec || compressionType != CompressionType.NONE) {
            codec = getCompressionCodec(context, getConfiguration());
        }

        final CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);

        for (final Path file : files) {
            try {
                if (!getUserGroupInformation().doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.exists(file))) {
                    // If the file is no longer there then move on.
                    continue;
                }

                final String originalFilename = file.getName();

                stream = getUserGroupInformation()
                        .doAs((PrivilegedExceptionAction<FSDataInputStream>) () -> hdfs.open(file, BUFFER_SIZE_DEFAULT));

                final String outputFilename;

                // Check if we should infer compression codec.
                if (inferCompressionCodec) {
                    codec = compressionCodecFactory.getCodec(file);
                }

                // Check if compression codec is defined (inferred or otherwise).
                if (codec != null) {
                    stream = codec.createInputStream(stream);
                    outputFilename = StringUtils.removeEnd(originalFilename, codec.getDefaultExtension());
                } else {
                    outputFilename = originalFilename;
                }

                // Create a FlowFile for the HDFS file, and keep the parent's attributes.
                FlowFile flowFile = session.create(parentFlowFile);

                final StopWatch stopWatch = new StopWatch(true);
                flowFile = session.importFrom(stream, flowFile);
                stopWatch.stop();
                final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());
                final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);

                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), rootDir.toString());
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), outputFilename);

                if (!keepSourceFiles && !getUserGroupInformation()
                        .doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.delete(file, false))) {
                    getLogger().warn("Could not remove {} from HDFS. Not ingesting this file ...",
                            new Object[]{file});
                    session.remove(flowFile);
                    continue;
                }

                session.transfer(flowFile, REL_SUCCESS);

                getLogger().info("Retrieved {} from HDFS {} in {} milliseconds at a rate of {}",
                        new Object[]{flowFile, file, millis, dataRate});

            } catch (final Throwable t) {
                getLogger().error("Error retrieving file {} from HDFS due to {}", new Object[]{file, t});
                context.yield();
                //noinspection UnusedAssignment
                parentFlowFile = session.putAttribute(parentFlowFile, "hdfs.failure.reason", t.getMessage());
                return false;
            } finally {
                IOUtils.closeQuietly(stream);
                stream = null;
            }
        }
        return true;
    }

    /**************************************************************
     * Select files to grab that match the configured file filters.
     *
     * @param hdfs
     *            hdfs
     * @param dir
     *            dir
     * @param filesVisited
     *            filesVisited
     * @return files to process
     * @throws java.io.IOException
     *             ex
     **************************************************************/
    @SuppressWarnings("SameParameterValue")
    protected Set<Path> selectFiles(final FileSystem hdfs, final Path dir, Set<Path> filesVisited)
            throws IOException, InterruptedException {

        if (null == filesVisited) {
            filesVisited = new HashSet<>();
        }

        if (!hdfs.exists(dir)) {
            throw new IOException("Selection directory " + dir.toString() + " doesn't appear to exist!");
        }

        final Set<Path> files = new HashSet<>();

        FileStatus[] fileStatuses = getUserGroupInformation()
                .doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(dir));

        for (final FileStatus file : fileStatuses) {

            final Path canonicalFile = file.getPath();

            if (!filesVisited.add(canonicalFile)) {
                // Skip files we've already seen (may be looping directory links).
                continue;
            }

            if (processorConfig.getPathFilter(dir).accept(canonicalFile)) {
                files.add(canonicalFile);
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug(this + " selected file at path: " + canonicalFile.toString());
                }
            }
        }
        return files;
    }

    /**************************************************************
     * Processor properties that are passed around.
     **************************************************************/
    protected static class ProcessorConfiguration {

        final private Pattern fileFilterPattern;
        final private boolean ignoreDottedFiles;

        ProcessorConfiguration(final ProcessContext context) {
            ignoreDottedFiles = context.getProperty(IGNORE_DOTTED_FILES).asBoolean();
            final String fileFilterRegex = context.getProperty(FILE_FILTER_REGEX).getValue();
            fileFilterPattern = (fileFilterRegex == null) ? null : Pattern.compile(fileFilterRegex);
        }

        protected PathFilter getPathFilter(final Path dir) {
            return path -> {
                if (ignoreDottedFiles && path.getName().startsWith(".")) {
                    return false;
                }
                final String pathToCompare;
                pathToCompare = path.getName();
                return fileFilterPattern == null || fileFilterPattern.matcher(pathToCompare).matches();
            };
        }
    }
}