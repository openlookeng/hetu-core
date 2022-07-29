/*
 * Copyright (C) 2022-2022. Yijian Cheng. All rights reserved.
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
package io.hetu.core.plugin.mpp.scheduler.entity;

public class TableSchema
{
    private String columns;
    private String gsSchema;
    private String hiveSchema;
    private String schemaTime;

    public TableSchema()
    {
    }

    public TableSchema(String columns, String gsSchema, String hiveSchema, String schemaTime)
    {
        this.columns = columns;
        this.gsSchema = gsSchema;
        this.hiveSchema = hiveSchema;
        this.schemaTime = schemaTime;
    }

    public String getColumns()
    {
        return columns;
    }

    public void setColumns(String columns)
    {
        this.columns = columns;
    }

    public String getGsSchema()
    {
        return gsSchema;
    }

    public void setGsSchema(String gsSchema)
    {
        this.gsSchema = gsSchema;
    }

    public String getHiveSchema()
    {
        return hiveSchema;
    }

    public void setHiveSchema(String hiveSchema)
    {
        this.hiveSchema = hiveSchema;
    }

    public String getSchemaTime()
    {
        return schemaTime;
    }

    public void setSchemaTime(String schemaTime)
    {
        this.schemaTime = schemaTime;
    }
}
