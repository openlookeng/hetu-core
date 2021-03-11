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
package io.hetu.core.plugin.greenplum;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class GreenPlumSqlConfig
{
    private ArrayMapping arrayMapping = ArrayMapping.DISABLED;

    private boolean allowModifyTable = true;

    public enum ArrayMapping
    {
        DISABLED,
        @Deprecated
        AS_ARRAY,
    }

    public boolean getAllowModifyTable()
    {
        return this.allowModifyTable;
    }

    @Config("greenplum.allow-modify-table")
    public GreenPlumSqlConfig setAllowModifyTable(boolean allowModifyTable)
    {
        this.allowModifyTable = allowModifyTable;
        return this;
    }

    @NotNull
    public ArrayMapping getArrayMapping()
    {
        return arrayMapping;
    }

    @Config("greenplum.experimental.array-mapping")
    public GreenPlumSqlConfig setArrayMapping(ArrayMapping arrayMapping)
    {
        this.arrayMapping = arrayMapping;
        return this;
    }
}
