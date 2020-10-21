/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.heuristicindex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IndexRecord
{
    public static final String COLUMN_DELIMITER = ",";
    public final String name;
    public final String user;
    public final String table;
    public final String[] columns;
    public final String indexType;
    public final List<String> partitions;

    public IndexRecord(String name, String user, String table, String[] columns, String indexType, List<String> partitions)
    {
        this.name = name;
        this.user = user == null ? "" : user;
        this.table = table;
        this.columns = columns;
        this.indexType = indexType;
        this.partitions = partitions;
    }

    public IndexRecord(String csvRecord)
    {
        String[] records = csvRecord.split("\\t");
        this.name = records[0];
        this.user = records[1];
        this.table = records[2];
        this.columns = records[3].split(COLUMN_DELIMITER);
        this.indexType = records[4];
        this.partitions = records.length > 5 ? Arrays.asList(records[5].split(",")) : Collections.emptyList();
    }

    public String toCsvRecord()
    {
        return String.format("%s\t%s\t%s\t%s\t%s\t%s\n", name, user, table, String.join(COLUMN_DELIMITER, columns), indexType, String.join(",", partitions));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexRecord)) {
            return false;
        }
        IndexRecord that = (IndexRecord) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(user, that.user) &&
                Objects.equals(table, that.table) &&
                Arrays.equals(columns, that.columns) &&
                Objects.equals(indexType, that.indexType);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(name, user, table, indexType);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    @Override
    public String toString()
    {
        return name + ","
                + user + ","
                + table + ","
                + "[" + String.join(",", columns) + "],"
                + indexType;
    }
}
