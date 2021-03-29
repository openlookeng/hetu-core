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

package io.hetu.core.materializedview.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * util of date
 *
 * @since 2020-03-28
 */
public class MaterializedViewDateUtils
{
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MaterializedViewDateUtils()
    {
    }

    /**
     * get date from dateSting format as "yyyy-MM-dd HH:mm:ss"
     *
     * @param dateString date string
     * @return Date
     */
    public static Date getDateFromString(String dateString)
    {
        try {
            return DATE_FORMAT.parse(dateString);
        }
        catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * get formatted String from date
     *
     * @param date date
     * @return string
     */
    public static String getStringFromDate(Date date)
    {
        return DATE_FORMAT.format(date);
    }
}
