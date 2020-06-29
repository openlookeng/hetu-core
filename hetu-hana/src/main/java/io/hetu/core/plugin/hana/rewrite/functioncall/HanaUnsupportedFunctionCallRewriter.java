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
package io.hetu.core.plugin.hana.rewrite.functioncall;

import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.builder.functioncall.functions.base.UnsupportedFunctionCallRewriter;

import static io.hetu.core.plugin.hana.rewrite.RewriteUtil.LITERAL_FUNCNAME_PREFIX;

/**
 * internal function call or build-in function that is completely not support for hanan datasource.
 * This is class is for rewrite inner function call to support hana expression push down
 *
 * @since 2019-10-10
 */

public class HanaUnsupportedFunctionCallRewriter
        extends UnsupportedFunctionCallRewriter
{
    /**
     * functioncall name of INTERVAL_DAY_TO_SECOND literal in HeTu inner
     */
    public static final String INNER_FUNC_INTERVAL_LITERAL_DAY2SEC =
            LITERAL_FUNCNAME_PREFIX + StandardTypes.INTERVAL_DAY_TO_SECOND;

    /**
     * functioncall name of INTERVAL_YEAR_TO_MONTH literal in HeTu inner
     */
    public static final String INNER_FUNC_INTERVAL_LITERAL_YEAR2MONTH =
            LITERAL_FUNCNAME_PREFIX + StandardTypes.INTERVAL_YEAR_TO_MONTH;

    /**
     * functioncall name of TIME_WITH_TIME_ZONE literal in HeTu inner
     */
    public static final String INNER_FUNC_TIME_WITH_TZ_LITERAL =
            LITERAL_FUNCNAME_PREFIX + StandardTypes.TIME_WITH_TIME_ZONE;

    /**
     * the constructor of Unsupported Function Call Re-writer
     *
     * @param connectorName the hana connector's name
     */
    public HanaUnsupportedFunctionCallRewriter(String connectorName)
    {
        super(connectorName);
    }
}
