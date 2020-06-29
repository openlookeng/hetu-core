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
package io.hetu.core.heuristicindex;

import io.hetu.core.spi.heuristicindex.DataSource;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class CsvDataSource
        implements DataSource
{
    private static final String ID = "csv";
    private static final String ROOT_DIR_KEY = "rootDir";

    private Properties properties;

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public void readSplits(String schema, String table, String[] columns, String[] partitions, Callback callback) throws IOException
    {
        String rootDirValue = getProperties().getProperty(ROOT_DIR_KEY);
        requireNonNull(rootDirValue, "no root dir set, set rootDir key in properties");

        Path tablePath = Paths.get(rootDirValue, schema, table);

        List<Path> pathsToRead = new LinkedList<>();

        if (partitions == null) {
            pathsToRead.add(tablePath);
        }
        else {
            for (String partition : partitions) {
                pathsToRead.add(Paths.get(tablePath.toString(), partition));
            }
        }

        for (Path path : pathsToRead) {
            Files.walk(path).filter(Files::isRegularFile).forEach(e -> {
                try {
                    Map<Integer, List<Object>> values = readCsv(e);

                    for (String column : columns) {
                        Integer columnNum = Integer.valueOf(column);

                        List<Object> columnValues = values.get(columnNum);

                        callback.call(column, columnValues.toArray(new Object[0]), e.toString(), 0, e.toFile().lastModified());
                    }
                }
                catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                catch (ClassNotFoundException ex) {
                    throw new IllegalStateException(ex);
                }
            });
        }
    }

    /**
     * <pre>
     * Reads a CSV file and returns a mapping from column number to column values
     *
     * Assumes a valid CSV file is provided.
     *
     * e.g. if csv file contains:
     *
     * john doe, 1980, 25
     * jane doe, 1900, 100
     *
     * output will be:
     *
     * 0 -> john doe, jane doe
     * 1 -> 1980, 1900
     * 2 -> 25, 100
     *
     * </pre>
     *
     * @param csvFile
     * @return
     * @throws IOException
     */
    private Map<Integer, List<Object>> readCsv(Path csvFile) throws IOException, ClassNotFoundException
    {
        Map<Integer, List<Object>> result = new HashMap<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFile.toFile()), StandardCharsets.UTF_8))) {
            String line = br.readLine();
            if (line == null) {
                throw new IllegalStateException("CSV file is empty: " + csvFile.toString());
            }
            String[] types = line.split(",");
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");

                for (int i = 0; i < columns.length; i++) {
                    List<Object> values = result.get(i);
                    if (values == null) {
                        values = new LinkedList<>();
                        result.put(i, values);
                    }

                    if (Class.forName(types[i]).equals(String.class)) {
                        values.add(columns[i]);
                    }
                    else if (Class.forName(types[i]).equals(Integer.class)) {
                        values.add(Integer.valueOf(columns[i]));
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Properties getProperties()
    {
        return properties;
    }

    @Override
    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }
}
