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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

public class HetuCarbondataReadSupport<T>
        implements CarbonReadSupport<T>
{
    private DataType[] dataTypes;

    @Override
    public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
    {
        dataTypes = new DataType[carbonColumns.length];
        int i = 0;
        for (CarbonColumn col : carbonColumns) {
            dataTypes[i] = col.getDataType();
            i++;
        }
    }

    @Override
    public T readRow(Object[] objects)
    {
        throw new RuntimeException("UnSupported Method");
    }

    public DataType[] getDataTypes()
    {
        return dataTypes;
    }
}
