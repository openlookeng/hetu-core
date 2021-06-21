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

import java.util.Objects;

import static io.prestosql.plugin.splitmanager.TableSplitUtil.generateTableFullName;

public class TableSplitConfig
        implements Comparable<TableSplitConfig>
{
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String splitField;
    private boolean isCalcStepEnable;
    private boolean isDataReadOnly;
    private Integer scanNodes;
    private Long fieldMinValue;
    private Long fieldMaxValue;

    public String getCatalogName()
    {
        return catalogName;
    }

    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getSplitField()
    {
        return splitField;
    }

    public void setSplitField(String splitField)
    {
        this.splitField = splitField;
    }

    public boolean isDataReadOnly()
    {
        return isDataReadOnly;
    }

    public void setDataReadOnly(boolean dataReadOnly)
    {
        isDataReadOnly = dataReadOnly;
    }

    public boolean isCalcStepEnable()
    {
        return isCalcStepEnable;
    }

    public void setCalcStepEnable(boolean calcStepEnable)
    {
        isCalcStepEnable = calcStepEnable;
    }

    public Integer getSplitCount()
    {
        return scanNodes;
    }

    public void setSplitCount(Integer scanNodes)
    {
        this.scanNodes = scanNodes;
    }

    public Long getFieldMinValue()
    {
        return fieldMinValue;
    }

    public void setFieldMinValue(Long fieldMinValue)
    {
        this.fieldMinValue = fieldMinValue;
    }

    public Long getFieldMaxValue()
    {
        return fieldMaxValue;
    }

    public void setFieldMaxValue(Long fieldMaxValue)
    {
        this.fieldMaxValue = fieldMaxValue;
    }

    @Override
    public int compareTo(TableSplitConfig o)
    {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(generateTableFullName(catalogName, schemaName, tableName));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof TableSplitConfig)) {
            return false;
        }

        TableSplitConfig other = (TableSplitConfig) obj;
        return this.hashCode() == other.hashCode();
    }
}
