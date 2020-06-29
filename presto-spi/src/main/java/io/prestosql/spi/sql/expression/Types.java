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
package io.prestosql.spi.sql.expression;

public class Types
{
    private Types()
    {
    }

    public enum FrameBoundType
    {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING
    }

    public enum WindowFrameType
    {
        RANGE, ROWS
    }

    public enum Quantifier
    {
        ALL,
        ANY,
        SOME,
    }

    public enum JoinType
    {
        INNER("INNER JOIN"),
        LEFT("LEFT JOIN"),
        RIGHT("RIGHT JOIN"),
        FULL("FULL JOIN"),
        CROSS("CROSS JOIN");

        private final String joinLabel;

        JoinType(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }
    }

    public enum SetOperator
    {
        UNION_ALL("UNION ALL"),
        UNION_DISTINCT("UNION DISTINCT"),

        EXCEPT_DISTINCT("EXCEPT DISTINCT"),
        INTERSECT_DISTINCT("INTERSECT DISTINCT");

        private final String label;

        SetOperator(String label)
        {
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }
    }
}
