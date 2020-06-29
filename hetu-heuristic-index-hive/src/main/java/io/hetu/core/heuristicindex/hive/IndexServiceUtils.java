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
package io.hetu.core.heuristicindex.hive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Util class for creating external index.
 */
public class IndexServiceUtils
{
    /**
     * Error message for an invalid table name
     */
    private static final String INVALID_TABLE_NAME_ERR_MSG = "fully qualified table name is invalid, expected 'catalog.schema.table'";

    /**
     * there are minimum three parts in "catalog.schema.table"1
     */
    private static final int MINIMUM_FULLY_QUALIFIED_TABLE_FORMAT_PARTS = 3;

    private static final int DATABASE_NAME_OFFSET = 2;

    private static final int TABLE_NAME_OFFSET = 1;

    private IndexServiceUtils()
    {
    }

    /**
     * format a string into path format, add file separator if it's missing
     *
     * @param path path need to be formatted
     * @return formatted path
     */
    public static String formatPathAsFolder(String path)
    {
        if (!path.endsWith(File.separator)) {
            return path + File.separator;
        }
        return path;
    }

    /**
     * check if a file with specific file path exist
     *
     * @param filePath filesPath that need to be checked
     */
    public static void isFileExisting(String filePath) throws IOException
    {
        File file = Paths.get(filePath).toFile();
        isFileExisting(file);
    }

    /**
     * check if a file with specific file path exist
     *
     * @param file file need to be checked
     */
    public static void isFileExisting(File file) throws IOException
    {
        checkArgument(file.exists(), String.format(Locale.ROOT, "%s is not found", file.getCanonicalPath()));
    }

    /**
     * load properties from a filePath
     *
     * @param propertyFilePath property file path
     * @return Property object which holds all properties
     * @throws IOException when property file does NOT exist
     */
    public static Properties loadProperties(String propertyFilePath) throws IOException
    {
        File propertyFile = Paths.get(propertyFilePath).toFile();
        return loadProperties(propertyFile);
    }

    /**
     * load properties from a file object
     *
     * @param propertyFile property file
     * @return Property object which holds all properties
     * @throws IOException when property file does NOT exist
     */
    public static Properties loadProperties(File propertyFile) throws IOException
    {
        try (InputStream is = new FileInputStream(propertyFile)) {
            Properties properties = new Properties();
            properties.load(is);
            return properties;
        }
    }

    /**
     * get files path with a specific suffix from a path array
     *
     * @param paths  paths array
     * @param suffix specific suffix
     * @return first path with specific suffix from that array or null if nothing found
     */
    public static String getPath(String[] paths, String suffix)
    {
        for (String path : paths) {
            if (path.endsWith(suffix)) {
                return path;
            }
        }
        return null;
    }

    /**
     * split the fully qualified table name into three components
     * [catalog, schema, table]
     *
     * @param fullyQualifiedTableName table name in the form "catalog.schema.table"
     * @return a string array of size 3 containing the valid catalogName, databaseName, and tableName in sequence
     */
    public static String[] getTableParts(String fullyQualifiedTableName)
    {
        String[] parts = fullyQualifiedTableName.split("\\.");

        checkArgument(parts.length >= MINIMUM_FULLY_QUALIFIED_TABLE_FORMAT_PARTS,
                INVALID_TABLE_NAME_ERR_MSG);

        String databaseName = parts[parts.length - DATABASE_NAME_OFFSET].trim();
        checkArgument(!databaseName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        String catalogName = fullyQualifiedTableName
                .substring(0, fullyQualifiedTableName.indexOf(databaseName) - 1).trim();
        checkArgument(!catalogName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        String tableName = parts[parts.length - TABLE_NAME_OFFSET];
        checkArgument(!catalogName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        return new String[]{catalogName, databaseName, tableName};
    }
}
