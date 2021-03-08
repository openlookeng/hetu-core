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

package io.hetu.core.heuristicindex.util;

import io.airlift.slice.Slice;
import io.hetu.core.common.util.SecurePathWhiteList;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.heuristicindex.TypeUtils.extractValueFromRowExpression;

/**
 * Util class for creating external index.
 */
public class IndexServiceUtils
{
    /**
     * Error message for an invalid table name
     */
    private static final String INVALID_TABLE_NAME_ERR_MSG =
            "fully qualified table name is invalid, expected 'catalog.schema.table'";

    /**
     * there are minimum three parts in "catalog.schema.table"1
     */
    private static final int FULLY_QUALIFIED_TABLE_FORMAT_PARTS = 3;

    private static final int CATALOG_NAME_INDEX = 0;

    private static final int DATABASE_NAME_INDEX = 1;

    private static final int TABLE_NAME_INDEX = 2;

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
    public static void isFileExisting(String filePath)
            throws IOException
    {
        try {
            checkArgument(!filePath.contains("../"),
                    filePath + "Path must be absolute and at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(filePath),
                    filePath + "Path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }

        File file = Paths.get(filePath).toFile();
        isFileExisting(file);
    }

    /**
     * load properties from a filePath
     *
     * @param propertyFilePath property file path
     * @return Property object which holds all properties
     * @throws IOException when property file does NOT exist
     */
    public static Properties loadProperties(String propertyFilePath)
            throws IOException
    {
        File propertyFile = Paths.get(propertyFilePath).toFile();
        return loadProperties(propertyFile);
    }

    /**
     * check if a file with specific file path exist
     *
     * @param file file need to be checked
     */
    public static void isFileExisting(File file)
            throws IOException
    {
        checkArgument(file.exists(), file.getCanonicalPath() + " not found");
    }

    /**
     * load properties from a file object
     *
     * @param propertyFile property file
     * @return Property object which holds all properties
     * @throws IOException when property file does NOT exist
     */
    public static Properties loadProperties(File propertyFile)
            throws IOException
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
     * @param paths paths array
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

        checkArgument(parts.length == FULLY_QUALIFIED_TABLE_FORMAT_PARTS,
                INVALID_TABLE_NAME_ERR_MSG);

        String catalogName = parts[CATALOG_NAME_INDEX].trim();
        checkArgument(!catalogName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        String databaseName = parts[DATABASE_NAME_INDEX].trim();
        checkArgument(!databaseName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        String tableName = parts[TABLE_NAME_INDEX];
        checkArgument(!tableName.isEmpty(), INVALID_TABLE_NAME_ERR_MSG);

        return new String[] {catalogName, databaseName, tableName};
    }

    /**
     * Returns the subset of properties that start with the prefix, prefix is removed.
     * <p>
     * All keys and values are also converted to Strings
     *
     * @param properties <code>Properties</code> object to extract properties with
     * @param prefix A String prefix of some property key
     * @return A new <code>Properties</code> object with all property key starting with the <code>prefix</code>
     * given and is present in <code>properties</code>,
     * and the value of each key is exactly the same as it is in <code>properties</code>
     */
    public static Properties getPropertiesSubset(Properties properties, String prefix)
    {
        Properties subsetProps = new Properties();
        properties.keySet().forEach(key -> {
            // convert all keys and values to string and remove any quotes around them
            String keyStr = toStringRemoveQuotes(key);
            if (keyStr.startsWith(prefix)) {
                subsetProps.put(keyStr.substring(keyStr.indexOf(prefix) + prefix.length()),
                        toStringRemoveQuotes(properties.get(key)));
            }
        });

        return subsetProps;
    }

    /**
     * Tar the files in a source directory into a tarball on remote filesystem
     * <p>
     * This method expects the source directory to have index files name as [offset].[indextype] (e.g. {@code 3.bloom}) in the srcDir,
     * and will tar them into one remote tar file named as [lastModifiedTime].tar with all index files.
     *
     * @param srcFs FileSystemClient for the source directory
     * @param targetFs FileSystemClient on which the tar will be created
     * @param srcDir the dir containing all index files
     * @param tarPath a tar file path, whose file name must be /path/to/orc/file/[lastModifiedTime].tar
     * @throws IOException when exception occurs during taring
     */
    public static void writeToHdfs(HetuFileSystemClient srcFs, HetuFileSystemClient targetFs, Path srcDir, Path tarPath)
            throws IOException
    {
        AtomicReference<String> tarFileName = new AtomicReference<>();
        Collection<File> filesToArchive = srcFs
                .list(srcDir)
                .map(Path::toFile)
                .collect(Collectors.toList());

        targetFs.createDirectories(tarPath.getParent());
        try (OutputStream out = targetFs.newOutputStream(tarPath)) {
            try (TarArchiveOutputStream o = new TarArchiveOutputStream(out)) {
                for (File f : filesToArchive) {
                    ArchiveEntry entry = o.createArchiveEntry(f, f.getName());
                    o.putArchiveEntry(entry);
                    if (f.isFile()) {
                        try (InputStream i = srcFs.newInputStream(f.toPath())) {
                            IOUtils.copy(i, o);
                        }
                    }
                    o.closeArchiveEntry();
                }
            }
        }
    }

    public static boolean matchCallExpEqual(Object expression, Function<Object, Boolean> matchingFunction)
    {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            Optional<OperatorType> operatorOptional = Signature.getOperatorType(((CallExpression) expression).getSignature().getName());

            Object value = extractValueFromRowExpression(callExp.getArguments().get(1));

            if (operatorOptional.isPresent() && operatorOptional.get() == EQUAL) {
                return matchingFunction.apply(value);
            }

            return true;
        }

        return true;
    }

    /**
     * get object as string and remove surrounding quotes if present
     *
     * @param input
     * @return
     */
    private static String toStringRemoveQuotes(Object input)
    {
        return input == null ? null : input.toString().replace("\"", "").replace("'", "");
    }

    public static GroupSerializer getSerializer(String type)
    {
        switch (type) {
            case "long":
            case "Long":
                return Serializer.LONG;
            case "Slice":
            case "String":
                return Serializer.STRING;
            case "int":
            case "Integer":
                return Serializer.INTEGER;
            case "float":
            case "Float":
                return Serializer.FLOAT;
            case "double":
            case "Double":
                return Serializer.DOUBLE;
            case "boolean":
            case "Boolean":
                return Serializer.BOOLEAN;
            case "BigDecimal":
                return Serializer.BIG_DECIMAL;
            case "Date":
                return Serializer.DATE;
        }
        throw new RuntimeException("Index is not supported for type: (" + type + ")");
    }

    public static String extractType(Object object)
    {
        if (object instanceof Long) {
            return "Long";
        }
        else if (object instanceof String) {
            return "String";
        }
        else if (object instanceof Integer) {
            return "Integer";
        }
        else if (object instanceof Boolean) {
            return "boolean";
        }
        else if (object instanceof Slice) {
            return "String";
        }
        else if (object instanceof Float) {
            return "Float";
        }
        else if (object instanceof Double) {
            return "Double";
        }
        else if (object instanceof BigDecimal) {
            return "BigDecimal";
        }
        else if (object instanceof Date) {
            return "Date";
        }
        else {
            throw new UnsupportedOperationException("Not a valid type to create index: " + object.getClass());
        }
    }
}
