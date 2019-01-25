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

import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;

public class GoGetterCacheCallable extends GoGetterCallable {

    // fields
    DistributedMapCacheClient cacheService;
    String cacheKey;

    /**************************************************************
     * Constructor
     **************************************************************/
    public GoGetterCacheCallable(String key, Object defaultValue, String toType, DistributedMapCacheClient cacheService,
                                 String cacheKey) {
        // fields
        this.key = key;
        this.defaultValue = defaultValue;
        this.toType = toType;
        this.cacheService = cacheService;
        this.cacheKey = cacheKey;
    }

    /**************************************************************
     * call
     **************************************************************/
    @Override
    public GoGetterCallResult call() {
        try {
            GoGetterCallResult retVal = new GoGetterCallResult();
            retVal.key = this.key;
            retVal.defaultValue = this.defaultValue;
            retVal.toType = this.toType;
            retVal.result = cacheService.get(cacheKey, FlowUtils.stringSerializer, FlowUtils.stringDeserializer);
            return retVal;
        } catch (Exception e) {
            GoGetterCallException ce = new GoGetterCallException("Task Failed.", e);
            ce.originalException = e;
            ce.failureAttributes.put("gog.failure.cache.key", cacheKey);
            throw ce;
        }
    }
}
