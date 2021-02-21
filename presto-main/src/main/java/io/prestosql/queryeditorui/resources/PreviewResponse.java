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
package io.prestosql.queryeditorui.resources;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class PreviewResponse
{
    private final List<Map<String, String>> columns;
    private final List<List<String>> data;
    private final Integer total;
    private final String fileName;

    public PreviewResponse(List<Map<String, String>> columns,
                           List<List<String>> data,
                           Integer total,
                           String fileName)
    {
        this.columns = columns;
        this.data = data;
        this.total = total;
        this.fileName = fileName;
    }

    @JsonProperty
    public List<Map<String, String>> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<List<String>> getData()
    {
        return data;
    }

    @JsonProperty
    public int getTotal()
    {
        return total;
    }

    @JsonProperty
    public String getFileName()
    {
        return fileName;
    }
}
