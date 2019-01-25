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

import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;

import java.util.Arrays;

@SuppressWarnings({"WeakerAccess", "unused"})
public final class HBaseResultRowHandler implements ResultHandler {

    // fields
    private final HBaseResults results = new HBaseResults();
    private int rowCount = 0;

    /**************************************************************
     * handle
     **************************************************************/
    @Override
    public void handle(byte[] resultRow, ResultCell[] resultCells) {
        rowCount += 1;
        HBaseResultRow row = new HBaseResultRow();
        row.setRowKey(resultRow);
        for (final ResultCell resultCell : resultCells) {
            HBaseResultCell cell = new HBaseResultCell();
            cell.setFamily(Arrays.copyOfRange(resultCell.getFamilyArray(), resultCell.getFamilyOffset(), resultCell.getFamilyLength() + resultCell.getFamilyOffset()));
            cell.setQualifier(Arrays.copyOfRange(resultCell.getQualifierArray(), resultCell.getQualifierOffset(), resultCell.getQualifierLength() + resultCell.getQualifierOffset()));
            cell.setValue(Arrays.copyOfRange(resultCell.getValueArray(), resultCell.getValueOffset(), resultCell.getValueLength() + resultCell.getValueOffset()));
            row.cells.add(cell);
            results.setLastCellValue(cell.valueBytes);
        }
        results.rows.add(row);
    }

    /**************************************************************
     * getRowCount
     **************************************************************/
    public int getRowCount() {
        return rowCount;
    }

    /**************************************************************
     * getResults
     **************************************************************/
    public HBaseResults getResults() {
        return results;
    }
}
