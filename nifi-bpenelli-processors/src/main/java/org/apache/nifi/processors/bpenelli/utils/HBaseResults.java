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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.UUID;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class HBaseResults {

    // constants
    public static final String FMT_TBL_FAM_QUAL = "tablename.family.qualifier";
    public static final String FMT_TBL_QUAL = "tablename.qualifier";
    public static final String FMT_FAM_QUAL = "family.qualifier";
    public static final String FMT_QUAL = "qualifier";

    // fields
    public final ArrayList<HBaseResultRow> rows = new ArrayList<>();
    public String rowKeyName = "row_key";
    public String tableName = null;
    public String lastCellValue = null;
    public byte[] lastCellValueBytes = null;
    public String emitFormat = FMT_QUAL;

    /**************************************************************
     * setLastCellValue
     **************************************************************/
    public void setLastCellValue(byte[] bytes) {
        this.lastCellValueBytes = bytes;
        this.lastCellValue = new String(bytes);
    }

    /**************************************************************
     * getRowCount
     **************************************************************/
    public int getRowCount() {
        return this.rows.size();
    }

    /**************************************************************
     * emitFlowFiles
     **************************************************************/
    public void emitFlowFiles(ProcessSession session, FlowFile flowFile, Relationship successRel) {

        final int fragCount = this.rows.size();
        int fragIndex = 0;
        String fragID = UUID.randomUUID().toString();

        // Iterate the result rows.
        for (HBaseResultRow row : this.rows) {
            FlowFile newFlowFile = session.create(flowFile);
            // Add the row key attribute.
            newFlowFile = session.putAttribute(newFlowFile, rowKeyName, row.rowKey);
            fragIndex++;
            // Iterate the result row cells.
            for (HBaseResultCell cell : row.cells) {
                StringBuilder attrName = new StringBuilder();
                switch (emitFormat) {
                    case FMT_TBL_FAM_QUAL:
                        attrName.append(this.tableName);
                        attrName.append(".");
                        attrName.append(cell.family);
                        attrName.append(".");
                        attrName.append(cell.qualifier);
                        break;
                    case FMT_TBL_QUAL:
                        attrName.append(this.tableName);
                        attrName.append(".");
                        attrName.append(cell.qualifier);
                        break;
                    case FMT_FAM_QUAL:
                        attrName.append(cell.family);
                        attrName.append(".");
                        attrName.append(cell.qualifier);
                        break;
                    default:
                        attrName.append(cell.qualifier);
                        break;
                }
                // Add an attribute to the new FlowFile for the cell.
                newFlowFile = session.putAttribute(newFlowFile, attrName.toString(), cell.value);
            }
            // Add fragment attributes to the new FlowFile.
            newFlowFile = session.putAttribute(newFlowFile, "fragment.identifier", fragID);
            newFlowFile = session.putAttribute(newFlowFile, "fragment.index", Integer.toString(fragIndex));
            newFlowFile = session.putAttribute(newFlowFile, "fragment.count", Integer.toString(fragCount));
            session.transfer(newFlowFile, successRel);
        }
    }
}
