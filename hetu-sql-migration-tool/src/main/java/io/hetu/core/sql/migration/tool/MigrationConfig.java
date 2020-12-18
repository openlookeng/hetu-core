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

package io.hetu.core.sql.migration.tool;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

public class MigrationConfig
{
    private static final String CONVERT_DECIMAL_LITERALS_AS_DOUBLE = "convertDecimalLiteralsAsDouble";

    private Properties properties = new Properties();

    public MigrationConfig(String configFile) throws IOException
    {
        if (configFile != null) {
            try (InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(configFile)))) {
                properties.load(inputStream);
            }
        }
    }

    public boolean isConvertDecimalAsDouble()
    {
        String value = properties.getProperty(CONVERT_DECIMAL_LITERALS_AS_DOUBLE, "false");
        return value.toLowerCase(Locale.ENGLISH).equals(Boolean.TRUE.toString());
    }
}
