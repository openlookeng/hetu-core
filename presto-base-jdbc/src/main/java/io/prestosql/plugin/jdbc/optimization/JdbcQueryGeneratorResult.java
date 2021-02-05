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
package io.prestosql.plugin.jdbc.optimization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class JdbcQueryGeneratorResult
{
    private final GeneratedSql generatedSql;
    private final JdbcQueryGeneratorContext context;

    public JdbcQueryGeneratorResult(
            GeneratedSql generatedSql,
            JdbcQueryGeneratorContext context)
    {
        this.generatedSql = generatedSql;
        this.context = context;
    }

    public GeneratedSql getGeneratedSql()
    {
        return generatedSql;
    }

    public JdbcQueryGeneratorContext getContext()
    {
        return context;
    }

    public static class GeneratedSql
    {
        private final String sql;
        private final boolean isPushDown;

        @JsonCreator
        public GeneratedSql(
                @JsonProperty("sql") String sql,
                @JsonProperty("isPushDown") boolean isPushDown)
        {
            this.sql = sql;
            this.isPushDown = isPushDown;
        }

        @JsonProperty("sql")
        public String getSql()
        {
            return sql;
        }

        @JsonProperty("isPushDown")
        public boolean isPushDown()
        {
            return isPushDown;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("sql", sql)
                    .add("isPushDown", isPushDown)
                    .toString();
        }
    }
}
