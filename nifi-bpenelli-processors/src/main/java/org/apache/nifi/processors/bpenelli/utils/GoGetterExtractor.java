package org.apache.nifi.processors.bpenelli.utils;

import groovy.json.JsonBuilder;
import groovy.sql.GroovyRowResult;
import groovy.sql.Sql;
import groovyjarjarcommonscli.MissingArgumentException;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

public class GoGetterExtractor {

    /**************************************************************
     * extract
     **************************************************************/
    @SuppressWarnings({"unchecked"})
    public static void extract(Map<String, Object> gogMap, String gogKey, ProcessSession session,
                               ProcessContext context, FlowFile flowFile,
                               DistributedMapCacheClient cacheService, DBCPService dbcpService,
                               HBaseClientService hbaseService) throws Exception {

        final Map<String, Object> valueMap = new TreeMap<>();
        List<Callable<GoGetterCallResult>> goGetterCalls = new ArrayList<>();

        for (final String key : gogMap.keySet()) {

            final Object expression = gogMap.get(key);

            // Handle simple type property.
            if (!(expression instanceof Map)) {
                if (expression == null) {
                    valueMap.put(key, null);
                    continue;
                }
                // Evaluate any supplied expression language.
                final String result = FlowUtils.evaluateExpression(context, flowFile, expression.toString());
                // Add the result to our value map.
                valueMap.put(key, result);
                continue;
            }

            // Handle complex type property.
            Object defaultValue = null;
            String result;
            Map<String, Object> propMap = (Map<String, Object>) expression;

            // Get default property.
            if (propMap.containsKey("default")) {
                defaultValue = propMap.get("default");
            }

            // Get to-type property.
            String toType = null;
            if (propMap.containsKey("to-type")) {
                toType = propMap.get("to-type").toString();
            }

            // Get value property.
            Object value = propMap.get("value");
            if (value == null || value.toString().isEmpty()) {
                valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                continue;
            }

            // Get value expression language result.
            result = FlowUtils.evaluateExpression(context, flowFile, value.toString());

            // If value result is null or empty then use default value.
            if (result == null || result.isEmpty()) {
                valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                continue;
            }

            // Get type property.
            final String valType = propMap.containsKey("type") ? propMap.get("type").toString() : "";

            // Type handler.
            switch (valType) {
                case "CACHE_KEY":
                case "CACHE":
                    // Get the value from a DistributedMapCacheClient source asynchronously.
                    GoGetterCacheCallable callable = new GoGetterCacheCallable(key, defaultValue, toType,
                            cacheService, result);
                    goGetterCalls.add(callable);
                    continue;
                case "HBASE_FILTER":
                case "HBASE_SCAN":
                case "HBASE":
                    // Get the value from a HBaseClientService source asynchronously.
                    if (!propMap.containsKey("hbase-table")) {
                        throw new MissingArgumentException("hbase-table argument missing for " + key);
                    }
                    final String hbaseTable = propMap.get("hbase-table").toString();
                    GoGetterHBaseCallable getHbase = new GoGetterHBaseCallable(key, defaultValue, toType, hbaseService,
                            hbaseTable, result);
                    goGetterCalls.add(getHbase);
                    continue;
                case "SQL":
                    Sql sql = null;
                    final String sqlText = result;
                    // Get the value from a DBCPService source.
                    try {
                        Connection conn = dbcpService.getConnection();
                        sql = new Sql(conn);
                        GroovyRowResult row = sql.firstRow(sqlText);
                        if (row != null) {
                            final Object col = row.getAt(0);
                            result = FlowUtils.getColValue(col, null);
                            if (result == null || result.isEmpty()) {
                                valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                                continue;
                            }
                        } else {
                            valueMap.put(key, FlowUtils.convertString(defaultValue, toType));
                            continue;
                        }
                    }
                    catch (Exception e) {
                        //noinspection UnusedAssignment
                        flowFile = session.putAttribute(flowFile, "gog.failure.sql", sqlText);
                        throw e;
                    } finally {
                        if (sql != null) sql.close();
                    }
                    break;
                default:
                    // No type specified, so value result is a literal.
                    break;
            }

            // Add the result to our value map, after any specified type conversion.
            valueMap.put(key, FlowUtils.convertString(result, toType));
        }

        if (goGetterCalls.size() > 0) {
            // Invoke and gather the results of the GoGetter async calls.
            ExecutorService executor = Executors.newWorkStealingPool();
            int runningCalls = goGetterCalls.size();
            try {
                List<Future<GoGetterCallResult>> futureList = executor.invokeAll(goGetterCalls);
                while (runningCalls > 0) {
                    for (Future<GoGetterCallResult> future : futureList) {
                        if (future.isDone()) {
                            GoGetterCallResult callResult = future.get();
                            if (callResult.result == null || callResult.result.isEmpty()) {
                                valueMap.put(callResult.key, FlowUtils.convertString(callResult.defaultValue,
                                        callResult.toType));
                            } else {
                                valueMap.put(callResult.key, FlowUtils.convertString(callResult.result,
                                        callResult.toType));
                            }
                            runningCalls--;
                        }
                    }
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof GoGetterCallException) {
                    GoGetterCallException ce = (GoGetterCallException) e.getCause();
                    for (String attName : ce.failureAttributes.keySet()) {
                        flowFile = session.putAttribute(flowFile, attName, ce.failureAttributes.get(attName));
                    }
                    throw ce.originalException;
                }
            } finally {
                // Shutdown the executor service.
                executor.shutdownNow();
            }
        }

        // Output the extracted results.
        if (Objects.equals(gogKey, "extract-to-json")) {
            // Build a JSON object for these results and put it in the FlowFile's content.
            final JsonBuilder builder = new JsonBuilder();
            builder.call(valueMap);
            FlowUtils.writeContent(session, flowFile, builder);
        }
        if (Objects.equals(gogKey, "extract-to-attributes")) {
            // Add FlowFile attributes for these results.
            for (final String key : valueMap.keySet()) {
                flowFile = session.putAttribute(flowFile, key, valueMap.get(key).toString());
            }
        }
    }
}
