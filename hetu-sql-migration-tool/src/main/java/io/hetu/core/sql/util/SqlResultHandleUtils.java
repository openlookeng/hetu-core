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
package io.hetu.core.sql.util;

import io.airlift.log.Logger;
import io.hetu.core.sql.migration.tool.Console;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.stream.Collectors;

import static io.hetu.core.sql.migration.Constants.CONVERTED_SQL;
import static io.hetu.core.sql.migration.Constants.ORIGINAL_SQL;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlResultHandleUtils
{
    private static final Logger log = Logger.get(Console.class);
    private static final int BUFFER_SIZE = 16384;
    private static final String HTML_TMPLATE_FILE_NAME = "htmlResultTemplate.html";

    private SqlResultHandleUtils() {}

    public static void writeToHtmlFile(JSONArray convertedSqls, String outputFile)
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream is = classLoader.getResourceAsStream(HTML_TMPLATE_FILE_NAME);
                InputStreamReader isr = new InputStreamReader(is, UTF_8);
                BufferedReader reader = new BufferedReader(isr);
                OutputStream out = new FileOutputStream(outputFile + ".html");
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8), BUFFER_SIZE)) {
            String htmlTextTemplate = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            htmlEscape(convertedSqls);
            String htmlFileText = htmlTextTemplate.replaceAll("\\$\\{conversionResultData}\\$", convertedSqls.toString());

            writer.write(htmlFileText);
        }
        catch (IOException | JSONException e) {
            log.error(format("Failed to write result to file because of exception:" + e.getLocalizedMessage()));
        }
        log.info(format("Result is saved to %s", outputFile));
    }

    private static void htmlEscape(JSONArray convertedSqls)
            throws JSONException
    {
        for (int i = 0; i < convertedSqls.length(); i++) {
            JSONObject row = convertedSqls.getJSONObject(i);
            // handle sql

            String sourceSql = convertSpecialCharsToHtmlChar(row.getString(ORIGINAL_SQL));
            String convertedSql = convertSpecialCharsToHtmlChar(row.getString(CONVERTED_SQL));
            row.put(ORIGINAL_SQL, sourceSql);
            row.put(CONVERTED_SQL, convertedSql);
        }
    }

    private static String convertSpecialCharsToHtmlChar(String inputString)
    {
        if (inputString == null || inputString.length() == 0) {
            return inputString;
        }
        StringBuilder outputString = new StringBuilder();
        for (int i = 0; i < inputString.length(); i++) {
            char c = inputString.charAt(i);
            switch (c) {
                case '\"':
                    outputString.append("\\\"");
                    break;
                case '\\':
                    outputString.append("\\\\");
                    break;
                case '/':
                    outputString.append("\\/");
                    break;
                case '\b':
                    outputString.append("\\b");
                    break;
                case '\f':
                    outputString.append("\\f");
                    break;
                case '\n':
                    outputString.append("\\n");
                    break;
                case '\r':
                    outputString.append("\\r");
                    break;
                case '\t':
                    outputString.append("\\t");
                    break;
                default:
                    outputString.append(c);
            }
        }
        return outputString.toString();
    }
}
