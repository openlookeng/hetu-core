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

package io.hetu.core.plugin.oracle;

/**
 * Oracle Data types as defined at:
 * https://docs.oracle.com/cd/E11882_01/server.112/e41085/sqlqr06002.htm#SQLQR959
 *
 * @since 2019-07-06
 */

public class OracleTypes
{
    /**
     * Variable-length character string having maximum length size bytes or characters.
     */
    public static final int VARCHAR2_OR_NVARCHAR2 = 1;

    /**
     * Number having precision p and scale s.
     */
    public static final int NUMBER_OR_FLOAT = 2;

    /**
     * Valid date range from January 1, 4712 BC, to December 31, 9999 AD.
     */
    public static final int DATE = 12;

    /**
     * Raw binary data of length size bytes
     */
    public static final int RAW = 23;

    /**
     * Raw binary data of variable length up to 2 gigabytes.
     */
    public static final int LONG_RAW = 24;

    /**
     * Fixed-length character data of length size bytes or characters.
     */
    public static final int CHAR_OR_NCHAR = 96;

    /**
     * 32-bit floating point number. This data type requires 4 bytes.
     */
    public static final int BINARY_FLOAT = 100;

    /**
     * 64-bit floating point number. This data type requires 8 bytes.
     */
    public static final int BINARY_DOUBLE = 101;

    /**
     * A character large object containing single-byte or multibyte characters.
     */
    public static final int CLOB_OR_NCLOB = 112;

    /**
     * A binary large object. Maximum size is (4 gigabytes - 1) * (database block size).
     */
    public static final int BLOB = 2004;

    /**
     * Year, month, and day values of date, as well as hour, minute, and second values
     */
    public static final int TIMESTAMP = 93;

    /**
     * Oracle data type of TIMESTAMP 6 WITH TIME ZONE
     */
    public static final int TIMESTAMP6_WITH_TIMEZONE = -101;

    /**
     * All values of TIMESTAMP as well as time zone displacement value
     */
    public static final int TIMESTAMP_WITH_TIMEZONE_OR_NCLOB_OR_NVARCHAR2 = 1111;

    /**
     * Oracle built-in data type LONG
     */
    public static final int LONG = -1;

    public static final int ROWID = -8;

    private OracleTypes()
    {
    }
}
