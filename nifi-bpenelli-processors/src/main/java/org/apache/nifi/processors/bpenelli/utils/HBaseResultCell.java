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

@SuppressWarnings({"WeakerAccess", "unused"})
public final class HBaseResultCell {

    // fields
    public String family;
    public byte[] familyBytes;
    public String qualifier;
    public byte[] qualifierBytes;
    public String value;
    public byte[] valueBytes;

    /**************************************************************
     * setFamily
     **************************************************************/
    public void setFamily(byte[] bytes) {
        this.familyBytes = bytes;
        this.family = new String(bytes);
    }

    /**************************************************************
     * setQualifier
     **************************************************************/
    public void setQualifier(byte[] bytes) {
        this.qualifierBytes = bytes;
        this.qualifier = new String(bytes);
    }

    /**************************************************************
     * setValue
     **************************************************************/
    public void setValue(byte[] bytes) {
        this.valueBytes = bytes;
        this.value = new String(bytes);
    }
}