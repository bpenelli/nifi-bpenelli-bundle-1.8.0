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
 *
 */

package org.apache.nifi.processors.bpenelli.utils;

import org.apache.nifi.hbase.HBaseClientService;

public class GoGetterHBaseCallable extends GoGetterCallable {

    // fields
    HBaseClientService hBaseService;
    String hbaseTable;
    String filterExpression;

    /**************************************************************
     * Constructor
     **************************************************************/
    public GoGetterHBaseCallable(String key, Object defaultValue, String toType, HBaseClientService hBaseService,
                                 String hbaseTable, String filterExpression) {
        // fields
        this.key = key;
        this.defaultValue = defaultValue;
        this.toType = toType;
        this.hBaseService = hBaseService;
        this.hbaseTable = hbaseTable;
        this.filterExpression = filterExpression;
    }

    /**************************************************************
     * call
     **************************************************************/
    @Override
    public GoGetterCallResult call() throws Exception {
        try {
            GoGetterCallResult retVal = new GoGetterCallResult();
            retVal.key = this.key;
            retVal.defaultValue = this.defaultValue;
            retVal.toType = this.toType;
            retVal.result = HBaseUtils.getLastCellValueByFilter(this.hBaseService, this.hbaseTable, this.filterExpression);
            return retVal;
        } catch (Exception e) {
            GoGetterCallException ce = new GoGetterCallException("Task Failed.", e);
            ce.originalException = e;
            ce.failureAttributes.put("gog.failure.hbase.filter", filterExpression);
            ce.failureAttributes.put("gog.failure.hbase.table", hbaseTable);
            throw ce;
        }
    }
}
