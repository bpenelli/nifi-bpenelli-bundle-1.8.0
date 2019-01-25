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

import java.util.ArrayList;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class HBaseResultRow {

    // fields
    public final ArrayList<HBaseResultCell> cells = new ArrayList<>();
    public String rowKey;
    public byte[] rowKeyBytes;

    /**************************************************************
     * setRowKey
     **************************************************************/
    public void setRowKey(byte[] bytes) {
        this.rowKeyBytes = bytes;
        this.rowKey = new String(bytes);
    }

    /**************************************************************
     * getCellValue
     **************************************************************/
    public String getCellValue(String family, String qualifier) {
        for (HBaseResultCell cell : this.cells) {
            if (cell.family.equals(family) && cell.qualifier.equals(qualifier)) {
                return cell.value;
            }
        }
        return null;
    }

    /**************************************************************
     * getCellValueBytes
     **************************************************************/
    public byte[] getCellValueBytes(String family, String qualifier) {
        for (HBaseResultCell cell : this.cells) {
            if (cell.family.equals(family) && cell.qualifier.equals(qualifier)) {
                return cell.valueBytes;
            }
        }
        return null;
    }
}
