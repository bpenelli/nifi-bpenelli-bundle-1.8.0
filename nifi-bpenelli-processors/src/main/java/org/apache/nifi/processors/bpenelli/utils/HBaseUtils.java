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
package org.apache.nifi.processors.bpenelli.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.scan.Column;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"WeakerAccess", "unused", "SpellCheckingInspection"})
public class HBaseUtils {

    /**************************************************************
     * Constructor
     **************************************************************/
    private HBaseUtils() {
    }

    // constants
    public static final String HBASE_CONF_ZK_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONF_ZK_PORT = "hbase.zookeeper.property.clientPort";
    public static final String HBASE_CONF_ZNODE_PARENT = "zookeeper.znode.parent";
    public static final String HBASE_CONF_CLIENT_RETRIES = "hbase.client.retries.number";

    /**************************************************************
     * serialize
     **************************************************************/
    public static <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(value, baos);
        return baos.toByteArray();
    }

    /**************************************************************
     * deserialize
     **************************************************************/
    public static <T> T deserialize(final byte[] value, final Deserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(value);
    }

    /**************************************************************
     * checkAndPut
     **************************************************************/
    public static <K, V> boolean checkAndPut(final HBaseClientService hbaseService, final String tableName,
                                             final String columnFamily, final String columnQualifier, final K key, final V value, final V checkValue,
                                             final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        final byte[] checkBytes = serialize(checkValue, valueSerializer);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

        return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, checkBytes, putColumn);
    }

    /**************************************************************
     * checkAndPut
     **************************************************************/
    public static boolean checkAndPut(final HBaseClientService hbaseService, final String tableName,
                                      final String columnFamily, final String columnQualifier, final String key, final String value, final String checkValue) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = hbaseService.toBytes(key);
        final byte[] valueBytes = hbaseService.toBytes(value);
        final byte[] checkBytes = hbaseService.toBytes(checkValue);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

        return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, checkBytes, putColumn);
    }

    /**************************************************************
     * putIfAbsent
     **************************************************************/
    public static <K, V> boolean putIfAbsent(final HBaseClientService hbaseService, final String tableName,
                                             final String columnFamily, final String columnQualifier, final K key, final V value,
                                             final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);

        return hbaseService.checkAndPut(tableName, rowIdBytes, columnFamilyBytes, columnQualifierBytes, null, putColumn);
    }

    /**************************************************************
     * put
     **************************************************************/
    public static <K, V> void put(final HBaseClientService hbaseService, final String tableName,
                                  final String columnFamily, final String columnQualifier, final K key, final V value,
                                  final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        List<PutColumn> putColumns = new ArrayList<>(1);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);
        putColumns.add(putColumn);

        hbaseService.put(tableName, rowIdBytes, putColumns);
    }

    /**************************************************************
     * put
     **************************************************************/
    public static void put(final HBaseClientService hbaseService, final String tableName,
                           final String columnFamily, final String columnQualifier, final String key, final String value) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        List<PutColumn> putColumns = new ArrayList<>(1);
        final byte[] rowIdBytes = hbaseService.toBytes(key);
        final byte[] valueBytes = hbaseService.toBytes(value);
        final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);
        putColumns.add(putColumn);

        hbaseService.put(tableName, rowIdBytes, putColumns);
    }

    /**************************************************************
     * containsKey
     **************************************************************/
    public static <K> boolean containsKey(final HBaseClientService hbaseService, final String tableName,
                                          final K key, final Serializer<K> keySerializer) throws IOException {

        final byte[] rowIdBytes = serialize(key, keySerializer);
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
        final List<Column> columnsList = new ArrayList<>(0);
        final List<String> stringList = new ArrayList<>(0);

        hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, stringList, handler);
        return (handler.getResults().getRowCount() > 0);
    }

    /**************************************************************
     * containsKey
     **************************************************************/
    public static boolean containsKey(final HBaseClientService hbaseService, final String tableName, final String key) throws IOException {

        final byte[] rowIdBytes = hbaseService.toBytes(key);
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
        final List<Column> columnsList = new ArrayList<>(0);
        final List<String> stringList = new ArrayList<>(0);
        hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, stringList, handler);
        return (handler.getResults().getRowCount() > 0);
    }

    /**************************************************************
     *  getAndPutIfAbsent
     **************************************************************
     *  Note that the implementation of getAndPutIfAbsent is not 
     *  atomic. The putIfAbsent is atomic, but a getAndPutIfAbsent
     *  does a get and then a putIfAbsent. If there is an existing
     *  value and it is updated in between the two steps, then the
     *  existing (unmodified) value will be returned. If the
     *  existing value was deleted between the two steps,
     *  getAndPutIfAbsent will correctly return null.
     **************************************************************/
    public static <K, V> V getAndPutIfAbsent(final HBaseClientService hbaseService, final String tableName,
                                             final String columnFamily, final String columnQualifier, final K key, final V value,
                                             final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                                             final Deserializer<V> valueDeserializer) throws IOException {

        // Between the get and the putIfAbsent, the value could be deleted or updated.
        // Logic below takes care of the deleted case but not the updated case.
        final V gotValue = HBaseUtils.get(hbaseService, tableName, columnFamily, columnQualifier, key, keySerializer, valueDeserializer);
        final boolean wasAbsent = HBaseUtils.putIfAbsent(hbaseService, tableName, columnFamily, columnQualifier, key, value, keySerializer, valueSerializer);

        if (!wasAbsent) return gotValue;
        else return null;
    }

    /**************************************************************
     * get
     **************************************************************/
    public static <K, V> V get(final HBaseClientService hbaseService, final String tableName,
                               final String columnFamily, final String columnQualifier, final K key,
                               final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {

        final byte[] columnFamilyBytes = hbaseService.toBytes(columnFamily);
        final byte[] columnQualifierBytes = hbaseService.toBytes(columnQualifier);
        final byte[] rowIdBytes = serialize(key, keySerializer);
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();

        final List<Column> columnsList = new ArrayList<>(0);
        Column col = new Column(columnFamilyBytes, columnQualifierBytes);
        columnsList.add(col);
        final List<String> stringList = new ArrayList<>(0);
        hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, stringList, handler);
        HBaseResults results = handler.getResults();

        if (results.getRowCount() > 1) {
            throw new IOException("Found multiple rows in HBase for key");
        } else if (results.getRowCount() == 1) {
            return deserialize(results.rows.get(0).getCellValueBytes(columnFamily, columnQualifier), valueDeserializer);
        } else {
            return null;
        }
    }

    /**************************************************************
     * get
     **************************************************************/
    public static String get(final HBaseClientService hbaseService, final String tableName,
                             final String columnFamily, final String columnQualifier, final String key) throws IOException {

        final byte[] rowIdBytes = hbaseService.toBytes(key);
        final List<Column> columnsList = new ArrayList<>(0);
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();
        final List<String> stringList = new ArrayList<>(0);

        hbaseService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, stringList, handler);
        HBaseResults results = handler.getResults();

        if (results.getRowCount() > 1) {
            throw new IOException("Found multiple rows in HBase for filter expression.");
        } else if (results.getRowCount() == 1) {
            return results.rows.get(0).getCellValue(columnFamily, columnQualifier);
        } else {
            return null;
        }
    }

    /**************************************************************
     * getByFilter
     **************************************************************/
    public static String getByFilter(final HBaseClientService hbaseService, final String tableName,
                                     final String columnFamily, final String columnQualifier, final String filterExpression) throws IOException {

        final List<Column> columnsList = new ArrayList<>(0);
        final long minTime = 0;
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();

        hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        HBaseResults results = handler.getResults();

        if (results.getRowCount() > 1) {
            throw new IOException("Found multiple rows in HBase for filter expression.");
        } else if (results.getRowCount() == 1) {
            return results.rows.get(0).getCellValue(columnFamily, columnQualifier);
        } else {
            return null;
        }
    }

    /**************************************************************
     * getLastCellValueByFilter
     **************************************************************/
    public static String getLastCellValueByFilter(final HBaseClientService hbaseService, final String tableName,
                                                  final String filterExpression) throws IOException {

        final List<Column> columnsList = new ArrayList<>(0);
        final long minTime = 0;
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();

        hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        HBaseResults results = handler.getResults();

        if (results.getRowCount() > 1) {
            throw new IOException("Found multiple rows in HBase for filter expression.");
        } else if (results.getRowCount() == 1) {
            return results.lastCellValue;
        } else {
            return null;
        }
    }

    /**************************************************************
     * scan
     **************************************************************/
    public static HBaseResults scan(final HBaseClientService hbaseService, final String tableName,
                                    final String filterExpression) throws IOException {

        final List<Column> columnsList = new ArrayList<>(0);
        final long minTime = 0;
        final HBaseResultRowHandler handler = new HBaseResultRowHandler();

        hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        HBaseResults results = handler.getResults();
        results.tableName = tableName;
        return results;
    }

    /**************************************************************
     * remove
     **************************************************************/
    public static <K> boolean remove(final HBaseClientService hbaseService, final String tableName,
                                     final K key, final Serializer<K> keySerializer) throws IOException {

        final boolean contains = HBaseUtils.containsKey(hbaseService, tableName, key, keySerializer);
        if (contains) {
            final byte[] rowIdBytes = serialize(key, keySerializer);
            hbaseService.delete(tableName, rowIdBytes);
        }
        return contains;
    }

    /**************************************************************
     * removeAll
     **************************************************************/
    public static void removeAll(final HBaseClientService hbaseService, final String tableName) throws IOException {

        final List<Column> columnsList = new ArrayList<>(0);
        final String filterExpression = "";
        final long minTime = 0;
        final HBaseDeleteRowHandler handler = new HBaseDeleteRowHandler(hbaseService, tableName);

        hbaseService.scan(tableName, columnsList, filterExpression, minTime, handler);
        if (handler.ioException != null) throw handler.ioException;
    }

    /**************************************************************
     * truncate
     **************************************************************/
    public static void truncate(final Connection connection, final String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName hbaseTableName = TableName.valueOf(tableName);
        admin.disableTable(hbaseTableName);
        admin.truncateTable(hbaseTableName, false);
    }

    /**************************************************************
     * createConnection
     **************************************************************/
    public static Connection createConnection(String configFiles,
                                              String zkQuorum, String zkPort, String zkZNodeParent,
                                              String hbaseClientRetries, Map<String, String> otherProperties ) throws IOException {

        final Configuration hbaseConfig = HBaseUtils.getConfigurationFromFiles(configFiles);

        // Override with any properties that are provided.
        if (zkQuorum != null && !zkQuorum.isEmpty()) {
            hbaseConfig.set(HBASE_CONF_ZK_QUORUM, zkQuorum);
        }
        if (zkPort != null && !zkPort.isEmpty()) {
            hbaseConfig.set(HBASE_CONF_ZK_PORT, zkPort);
        }
        if (zkZNodeParent != null && !zkZNodeParent.isEmpty()) {
            hbaseConfig.set(HBASE_CONF_ZNODE_PARENT, zkZNodeParent);
        }
        if (hbaseClientRetries != null && !hbaseClientRetries.isEmpty()) {
            hbaseConfig.set(HBASE_CONF_CLIENT_RETRIES, hbaseClientRetries);
        }

        // Add (or override) any other properties given to the HBase configuration.
        for (String key : otherProperties.keySet()) {
            hbaseConfig.set(key, otherProperties.get(key));
        }

        return ConnectionFactory.createConnection(hbaseConfig);
    }

    /**************************************************************
     * getConfigurationFromFiles
     **************************************************************/
    public static Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }
}