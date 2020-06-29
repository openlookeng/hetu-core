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

public class Time
{
    private Time()
    {
    }

    public enum IntervalSign
    {
        POSITIVE {
            @Override
            public int multiplier()
            {
                return 1;
            }
        },
        NEGATIVE {
            @Override
            public int multiplier()
            {
                return -1;
            }
        };

        public abstract int multiplier();
    }

    public enum IntervalField
    {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }

    public enum Function
    {
        TIME, DATE, TIMESTAMP, LOCALTIME, LOCALTIMESTAMP
    }

    public enum ExtractField
    {
        YEAR, QUARTER, MONTH, WEEK, DAY, DAY_OF_MONTH, DAY_OF_WEEK, DOW, DAY_OF_YEAR, DOY, YEAR_OF_WEEK, YOW, HOUR, MINUTE, SECOND, TIMEZONE_MINUTE, TIMEZONE_HOUR
    }
}
