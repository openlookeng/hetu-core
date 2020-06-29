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

import io.airlift.log.Logger;
import io.hetu.core.plugin.hana.HanaConfig;
import io.hetu.core.plugin.hana.rewrite.RewriteUtil;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimeZoneNotSupportedException;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import static io.hetu.core.plugin.hana.rewrite.RewriteUtil.formatQualifiedName;
import static io.hetu.core.plugin.hana.rewrite.RewriteUtil.printTimeWithoutTimeZone;
import static io.hetu.core.plugin.hana.rewrite.RewriteUtil.printTimestampWithoutTimeZone;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.lang.String.format;

/**
 *
 *
 * Time(without TZ) literal functionall:
 *
 * USE HeTu's legacy_timestamp condition:
 * For more information see: Hetu SQL grammar timestamp
 *   ----- user input ------------ rewriteFunctionCall's value ----- to hana -----
 * 1.1)time literal without TZ => epoch MS base TZ A => time literal with TZ
 *                                                                  or time literal convert to server's TZ
 *    client's session TZ A
 * 1.2)time literal with TZ B => epoch MS base TZ B => time literal with TZ
 *                                                                  or time literal convert to server's TZ
 *    client's session TZ A
 *
 * NEED: We can't get TZ information from a epoch ms value, so need to get context from SubQuery-Pushdown Framwork.
 *
 * USE sql standard condition:
 *   ----- user input --------------- rewriteFunctionCall's value ----- to hana -----
 * 2.1)time literal without TZ => epoch MS base TZ UTC => time literal with TZ
 *                                                                        or time literal convert to server's TZ
 * 2.2)time literal with TZ => error with input => NA
 *
 * TimeStamp(without TZ) literal functionall:
 *
 *
 * epoch MS base TZ B: TIME TZB => TIME UTC => epoch MS
 *
 * @since 2019-09-29
 */
public class DateTimeFunctionCallRewriter
        implements FunctionCallRewriter
{
    /**
     *  time without timezone literal function name
     */
    public static final String INNER_FUNC_TIME_LITERAL = "$literal$" + StandardTypes.TIME;

    /**
     *  timestamp without timezone literal function name
     */
    public static final String INNER_FUNC_TIMESTAMP_LITERAL = "$literal$" + StandardTypes.TIMESTAMP;

    private final Logger logger = Logger.get(DateTimeFunctionCallRewriter.class);

    private boolean isByPassHanaTzConvert = true;

    // hardcode NEED to get from context of calltrace
    private TimeZoneKey hetuTimeZone = TimeZoneKey.UTC_KEY;

    private TimeZoneKey dataSourceTimeZone = TimeZoneKey.UTC_KEY;

    /**
     * Constractoor
     * @param hanaConfig datasource config
     */
    public DateTimeFunctionCallRewriter(HanaConfig hanaConfig)
    {
        this.isByPassHanaTzConvert = hanaConfig.isByPassDataSourceTimeZone();
        try {
            dataSourceTimeZone = getTimeZoneKey(hanaConfig.getDataSourceTimeZoneKey());
        }
        catch (TimeZoneNotSupportedException e) {
            logger.warn("hana.query.datasourceTimeZoneKey error %s:", e.getMessage());
            // ignore the config error, to default value
            dataSourceTimeZone = TimeZoneKey.UTC_KEY;
        }
    }

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        if (formatQualifiedName(functionCallArgsPackage.getName()).equals(INNER_FUNC_TIME_LITERAL)) {
            long innerEpochMs = Long.parseLong(functionCallArgsPackage.getArgumentsList().get(0));

            // just support 2.1 scenario
            long userInputAsUtcEpochMs = innerEpochMs;

            String timeLiteral = isByPassHanaTzConvert ? printTimeWithoutTimeZone(userInputAsUtcEpochMs) : printTimeWithoutTimeZone(dataSourceTimeZone, userInputAsUtcEpochMs);

            return format("time'%s'", timeLiteral);
        }
        else if (formatQualifiedName(functionCallArgsPackage.getName()).equals(INNER_FUNC_TIMESTAMP_LITERAL)) {
            long innerEpochMs = Long.parseLong(functionCallArgsPackage.getArgumentsList().get(0));

            // just support 2.1 scenario
            long userInputAsUtcEpochMs = innerEpochMs;

            String timeStampLiteral = isByPassHanaTzConvert ? printTimestampWithoutTimeZone(userInputAsUtcEpochMs) : printTimestampWithoutTimeZone(dataSourceTimeZone, userInputAsUtcEpochMs);

            return format("timestamp'%s'", timeStampLiteral);
        }

        throw new UnsupportedOperationException("Hana Connector is not known to be supported function call of " + RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName()));
    }
}
