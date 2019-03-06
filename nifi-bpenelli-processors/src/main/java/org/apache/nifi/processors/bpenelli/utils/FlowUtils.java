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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class FlowUtils {

    /**************************************************************
     * Constructor
     **************************************************************/
    private FlowUtils() {
    }

    /**************************************************************
     * stringSerializer
     **************************************************************/
    public final static Serializer<String> stringSerializer = (stringValue, out) -> out.write(stringValue.getBytes(StandardCharsets.UTF_8));

    /**************************************************************
     * stringDeserializer
     **************************************************************/
    public final static Deserializer<String> stringDeserializer = String::new;

    /**************************************************************
     * applyCase
     **************************************************************/
    public static String applyCase(final String stringVal, final String toCase) {
        switch (toCase) {
            case "Upper":
                return stringVal.toUpperCase();
            case "Lower":
                return stringVal.toLowerCase();
            default:
                return stringVal;
        }
    }

    /**************************************************************
     * applyColMap
     **************************************************************/
    public static String applyColMap(final ProcessContext context, final FlowFile flowFile, final String sourceTableName,
                                     final String sourceColName, final String toCase) {
        final String colMapKey = sourceTableName + "." + sourceColName;
        String colName = sourceColName;
        for (PropertyDescriptor p : context.getProperties().keySet()) {
            if (p.isDynamic() && Objects.equals(p.getName(), colMapKey)) {
                PropertyValue propVal = context.getProperty(p);
                colName = propVal.evaluateAttributeExpressions(flowFile).getValue();
                break;
            }
        }
        colName = applyCase(colName, toCase);
        return colName;
    }

    /**************************************************************
     * convertString
     **************************************************************/
    public static Object convertString(final String value, final String newType) {
        if (value == null || newType == null) return value;
        Object converted;
        switch (newType) {
            case "int":
                converted = Integer.parseInt(value);
                break;
            case "long":
                converted = Long.parseLong(value);
                break;
            case "float":
                converted = Float.parseFloat(value);
                break;
            case "decimal":
            case "double":
                converted = Double.parseDouble(value);
                break;
            case "string":
            default:
                converted = value;
                break;
        }
        return converted;
    }

    /**************************************************************
     * convertString
     **************************************************************/
    public static Object convertString(final Object value, final String newType) {
        if (value == null || newType == null) return value;
        return FlowUtils.convertString(value.toString(), newType);
    }

    /**************************************************************
     * evaluateExpression
     **************************************************************/
    public static String evaluateExpression(final ProcessContext context, final FlowFile flowFile, final String expression) {
        PropertyValue newPropVal = context.newPropertyValue(expression);
        return newPropVal.evaluateAttributeExpressions(flowFile).getValue();
    }

    /**************************************************************
     * getColValue
     **************************************************************/
    public static String getColValue(final Object col, final String defaultValue) throws SQLException, IOException {
        String result;
        if (col instanceof Clob) {
            Reader stream = ((Clob) col).getCharacterStream();
            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer);
            result = writer.toString();
        } else {
            result = col != null ? col.toString() : defaultValue;
        }
        return result;
    }

    /**************************************************************
     * getDynamicPropertyMap
     **************************************************************/
    public static Map<String, String> getDynamicPropertyMap(ProcessContext context, FlowFile flowFile) {
        Map<String, String> retVal = new HashMap<>();
        for (PropertyDescriptor propDesc : context.getProperties().keySet()) {
            if (propDesc.isDynamic()) {
                PropertyValue propVal = context.getProperty(propDesc);
                final String value = propVal.evaluateAttributeExpressions(flowFile).getValue();
                retVal.put(propDesc.getName(), value);
            }
        }
        return retVal;
    }

    /**************************************************************
     * readContent
     **************************************************************/
    public static AtomicReference<String> readContent(final ProcessSession session, final FlowFile flowFile) {
        final AtomicReference<String> content = new AtomicReference<>();
        session.read(flowFile, inputStream -> content.set(IOUtils.toString(inputStream, StandardCharsets.UTF_8)));
        return content;
    }

    /**************************************************************
     * writeContent
     **************************************************************/
    public static FlowFile writeContent(final ProcessSession session, FlowFile flowFile, final Object content) {
        flowFile = session.write(flowFile, outputStream -> outputStream.write(content.toString().getBytes(StandardCharsets.UTF_8)));
        return flowFile;
    }
}
