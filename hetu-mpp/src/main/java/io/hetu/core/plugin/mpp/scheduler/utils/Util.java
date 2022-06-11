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
package io.hetu.core.plugin.mpp.scheduler.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * @author chengyijian
 * @title: Util
 * @projectName mpp-scheduler
 * @description: TODO
 * @date 2021/8/1713:54
 */
public class Util
{
    private Util()
    {
    }

    public static String getDate()
    {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        return formatter.format(date);
    }

    /**
     * GaussDB 数据类型映射为Hive表的数据类型
     *
     * @param gsType
     * @return
     */
    public static String getMappingHiveType(String gsType, int columnSize, int decimalDigits)
    {
        String hType = "string";

        if (Pattern.matches("int.*", gsType)) {
            hType = "int";
        }
        else if (gsType.equals("float")) {
            hType = "float";
        }
        else if (gsType.equals("double")) {
            hType = "double";
        }
        else if (gsType.equals("numeric")) {
            hType = "double";
        }
        else if (gsType.equals("date")) {
            hType = "date";
        }
        else if (gsType.equals("bpchar")) {
            hType = "string";
        }
        else if (gsType.equals("varchar")) {
            hType = "string";
        }
        return hType;
    }

    public static String getMappingGSType(String gsType, int columnSize, int decimalDigits)
    {
        String hType = "character varying(100)";

        if (Pattern.matches("int.*", gsType)) {
            hType = "integer";
        }
        else if (gsType.equals("float")) {
            hType = "float";
        }
        else if (gsType.equals("double")) {
            hType = "double";
        }
        else if (gsType.equals("numeric")) {
            hType = "numeric(" + columnSize + "," + decimalDigits + ")";
        }
        else if (gsType.equals("date")) {
            hType = "date";
        }
        else if (gsType.equals("bpchar")) {
            hType = "character(" + columnSize + ")";
        }
        else if (gsType.equals("varchar")) {
            hType = "character varying(" + columnSize + ")";
        }
        return hType;
    }

    public static String getGaussDBTable(String statement)
    {
        statement = statement.replaceAll("[\\t\\n\\r]", " ").replaceAll("\\,", " ,");
        if (Pattern.matches(".*\\/\\*\\*gds\\*\\*\\/.*", statement)) {
            statement = statement.replaceAll("\\/\\*\\*gds\\*\\*\\/", "");

            String[] tokens = statement.split(" ");
            for (String token : tokens) {
                if (Pattern.matches("gaussdb\\..*", token)) {
                    return token;
                }
            }
        }
        return null;
    }
}
