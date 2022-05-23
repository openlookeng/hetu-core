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
package io.prestosql.spi.favorite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class FavoriteResult
{
    @JsonProperty
    private long total;
    @JsonProperty
    private List<FavoriteEntity> queries;

    @JsonCreator
    public FavoriteResult(@JsonProperty("total") int total, @JsonProperty("queries") List<FavoriteEntity> queries)
    {
        this.total = total;
        this.queries = queries;
    }

    @JsonCreator
    public FavoriteResult()
    {
    }

    public void setTotal(long total)
    {
        this.total = total;
    }

    public void setQueries(List<FavoriteEntity> queries)
    {
        this.queries = queries;
    }

    @JsonProperty
    public long getTotal()
    {
        return total;
    }

    @JsonProperty
    public List<FavoriteEntity> getQueries()
    {
        return queries;
    }
}
