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
package io.hetu.core.metastore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Maps.fromProperties;

public class MetastoreUtFileLoader
{
    private static final MetastoreUtFileLoader metastoreUtFileLoader = new MetastoreUtFileLoader();

    private MetastoreUtFileLoader()
    {
    }

    /**
     * to load properties from file name
     *
     * @param fileName file name
     * @return key-value map
     * @throws IOException IOException
     */
    public Map<String, String> loadProperties(String fileName)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    /**
     * get the instance of MetastoreUtFileLoader
     *
     * @return an instance of MetastoreUtFileLoader
     */
    public static MetastoreUtFileLoader getInstance()
    {
        return metastoreUtFileLoader;
    }
}
