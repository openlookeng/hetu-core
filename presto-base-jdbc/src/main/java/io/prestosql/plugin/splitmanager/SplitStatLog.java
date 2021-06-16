/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.splitmanager;

public class SplitStatLog
        implements Comparable<SplitStatLog>
{
    public enum LogState
    {
        STATE_NEW,
        STATE_RUN_HISTORY,
        STATE_FINISH,
        STATE_CALC_HISTORY
    }

    private String catalogName;
    private String schemaName;
    private String tableName;
    private Long rows;
    private Long beginIndex;
    private Long endIndex;
    private long timeStamp;
    private LogState recordFlag;
    private Integer scanNodes;
    private String splitField;

    public String getCatalogName()
    {
        return catalogName;
    }

    public SplitStatLog setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public SplitStatLog setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SplitStatLog setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public Long getRows()
    {
        return rows;
    }

    public SplitStatLog setRows(Long rows)
    {
        this.rows = rows;
        return this;
    }

    public Long getBeginIndex()
    {
        return beginIndex;
    }

    public SplitStatLog setBeginIndex(Long beginIndex)
    {
        this.beginIndex = beginIndex;
        return this;
    }

    public Long getEndIndex()
    {
        return endIndex;
    }

    public SplitStatLog setEndIndex(Long endIndex)
    {
        this.endIndex = endIndex;
        return this;
    }

    public LogState getRecordFlag()
    {
        return recordFlag;
    }

    public SplitStatLog setRecordFlag(LogState recordFlag)
    {
        this.recordFlag = recordFlag;
        return this;
    }

    public Integer getScanNodes()
    {
        return scanNodes;
    }

    public SplitStatLog setScanNodes(Integer scanNodes)
    {
        this.scanNodes = scanNodes;
        return this;
    }

    public Long getTimeStamp()
    {
        return timeStamp;
    }

    public SplitStatLog setTimeStamp(Long timeStamp)
    {
        this.timeStamp = timeStamp;
        return this;
    }

    public String getSplitField()
    {
        return splitField;
    }

    public SplitStatLog setSplitField(String splitField)
    {
        this.splitField = splitField;
        return this;
    }

    @Override
    public int compareTo(SplitStatLog o)
    {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public String toString()
    {
        return getCatalogName() + "-"
                + getSchemaName() + "-"
                + getTableName() + "-"
                + getBeginIndex() + "-"
                + getEndIndex() + "-";
    }
}
