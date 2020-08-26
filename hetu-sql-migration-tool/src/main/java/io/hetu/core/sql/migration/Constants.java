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
package io.hetu.core.sql.migration;

public final class Constants
{
    public static final String ORIGINAL_SQL = "originalSql";
    public static final String ORIGINAL_SQL_TYPE = "originalSqlType";
    public static final String CONVERTED_SQL = "convertedSql";
    public static final String STATUS = "status";
    public static final String MESSAGE = "message";
    public static final String DIFFS = "diffs";

    public static final String SUCCESS = "Success";
    public static final String WARNING = "Warning";
    public static final String FAILED = "Failed";
    public static final String UNSUPPORTED = "Unsupported";

    // key words for properties
    public static final String PARTITIONED_BY = "partitioned_by";
    public static final String SORTED_BY = "sorted_by";
    public static final String FORMAT = "format";
    public static final String LOCATION = "location";
    public static final String TRANSACTIONAL = "transactional";

    public static final String HIVE = "hive";
    public static final String IMPALA = "impala";

    private Constants()
    {
    }
}
