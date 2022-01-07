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
package io.prestosql.spi.queryhistory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class QueryHistoryResult
{
    @JsonProperty
    private long total;
    @JsonProperty
    private List<QueryHistoryEntity> queries;

    @JsonCreator
    public QueryHistoryResult(@JsonProperty("total") int total,
                      @JsonProperty("queries") List<QueryHistoryEntity> queries)
    {
        this.total = total;
        this.queries = queries;
    }

    @JsonCreator
    public QueryHistoryResult()
    {
    }

    @JsonProperty
    public long getTotal()
    {
        return total;
    }

    @JsonProperty
    public List<QueryHistoryEntity> getQueries()
    {
        return queries;
    }

    public void setTotal(long total)
    {
        this.total = total;
    }

    public void setQueries(List<QueryHistoryEntity> queries)
    {
        this.queries = queries;
    }
}
