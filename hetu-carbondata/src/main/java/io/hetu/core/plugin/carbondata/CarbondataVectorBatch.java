/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.plugin.carbondata;

import io.hetu.core.plugin.carbondata.readers.BooleanStreamReader;
import io.hetu.core.plugin.carbondata.readers.ByteStreamReader;
import io.hetu.core.plugin.carbondata.readers.DecimalSliceStreamReader;
import io.hetu.core.plugin.carbondata.readers.DoubleStreamReader;
import io.hetu.core.plugin.carbondata.readers.FloatStreamReader;
import io.hetu.core.plugin.carbondata.readers.IntegerStreamReader;
import io.hetu.core.plugin.carbondata.readers.LongStreamReader;
import io.hetu.core.plugin.carbondata.readers.ObjectStreamReader;
import io.hetu.core.plugin.carbondata.readers.ShortStreamReader;
import io.hetu.core.plugin.carbondata.readers.SliceStreamReader;
import io.hetu.core.plugin.carbondata.readers.TimestampStreamReader;
import io.prestosql.plugin.hive.HiveColumnHandle;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CarbondataVectorBatch
{
    private static final int DEFAULT_BATCH_SIZE = 4 * 1024;

    private final int capacity;
    private final CarbonColumnVectorImpl[] columns;
    // True if the row is filtered.
    private final boolean[] filteredRows;
    // Column indices that cannot have null values.
    private final Set<Integer> nullFilteredColumns;
    private int numRows;
    // Total number of rows that have been filtered.
    private int numRowsFiltered;

    private CarbondataVectorBatch(StructField[] schema, HetuCarbondataReadSupport readSupport,
                                  int maxRows, List<HiveColumnHandle> columnHandle)
    {
        this.capacity = maxRows;
        this.columns = new CarbonColumnVectorImpl[schema.length];
        this.nullFilteredColumns = new HashSet<>();
        this.filteredRows = new boolean[maxRows];
        DataType[] dataTypes = readSupport.getDataTypes();

        for (int i = 0; i < schema.length; ++i) {
            boolean isCharType = columnHandle.get(i).getHiveType().getTypeInfo() instanceof CharTypeInfo;
            int maxLength = isCharType ? ((CharTypeInfo) columnHandle.get(i).getHiveType().getTypeInfo()).getLength() : -1;
            columns[i] = createDirectStreamReader(maxRows, dataTypes[i], schema[i], maxLength, isCharType);
        }

        numRowsFiltered = 0;
    }

    public static CarbondataVectorBatch allocate(StructField[] schema,
                                                 HetuCarbondataReadSupport readSupport, boolean isDirectFill, List<HiveColumnHandle> columnHandle)
    {
        if (isDirectFill) {
            return new CarbondataVectorBatch(schema, readSupport,
                    CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT, columnHandle);
        }
        else {
            return new CarbondataVectorBatch(schema, readSupport, DEFAULT_BATCH_SIZE, columnHandle);
        }
    }

    public static CarbonColumnVectorImpl createDirectStreamReader(int batchSize, DataType dataType,
            StructField field, int maxLength, boolean isCharType)
    {
        if (dataType == DataTypes.BOOLEAN) {
            return new BooleanStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.SHORT) {
            return new ShortStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.INT || dataType == DataTypes.DATE) {
            return new IntegerStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.TIMESTAMP) {
            return new TimestampStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.LONG) {
            return new LongStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.DOUBLE) {
            return new DoubleStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.FLOAT) {
            return new FloatStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.BYTE) {
            return new ByteStreamReader(batchSize, field.getDataType());
        }
        else if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR || dataType == DataTypes.BINARY) {
            return new SliceStreamReader(batchSize, field.getDataType(), maxLength, isCharType);
        }
        else if (DataTypes.isDecimal(dataType)) {
            if (dataType instanceof DecimalType) {
                return new DecimalSliceStreamReader(batchSize, field.getDataType(), (DecimalType) dataType);
            }
            else {
                return null;
            }
        }
        else {
            return new ObjectStreamReader(batchSize, field.getDataType());
        }
    }

    /**
     * Resets the batch for writing.
     */
    public void reset()
    {
        for (int i = 0; i < numCols(); ++i) {
            columns[i].reset();
        }
        if (this.numRowsFiltered > 0) {
            Arrays.fill(filteredRows, false);
        }
        this.numRows = 0;
        this.numRowsFiltered = 0;
    }

    /**
     * Returns the number of columns that make up this batch.
     */
    public int numCols()
    {
        return columns.length;
    }

    /**
     * Sets the number of rows that are valid. Additionally, marks all rows as "filtered" if one or
     * more of their attributes are part of a non-nullable column.
     */
    public void setNumRows(int numRows)
    {
        if (numRows > this.capacity) {
            throw new RuntimeException("Num Rows is greater than the capacity");
        }

        this.numRows = numRows;

        for (int ordinal : nullFilteredColumns) {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (!filteredRows[rowId] && columns[ordinal].isNull(rowId)) {
                    filteredRows[rowId] = true;
                    ++numRowsFiltered;
                }
            }
        }
    }

    /**
     * Returns the number of rows for read, including filtered rows.
     */
    public int numRows()
    {
        return numRows;
    }

    /**
     * Returns the number of valid rows.
     */
    public int numValidRows()
    {
        if (numRowsFiltered > numRows) {
            throw new RuntimeException("numRowsFiltered is greater than the numRows");
        }

        return numRows - numRowsFiltered;
    }

    /**
     * Returns the column at `ordinal`.
     */
    public CarbonColumnVectorImpl column(int ordinal)
    {
        return columns[ordinal];
    }

    /**
     * Returns the max capacity (in number of rows) for this batch.
     */
    public int capacity()
    {
        return capacity;
    }
}
