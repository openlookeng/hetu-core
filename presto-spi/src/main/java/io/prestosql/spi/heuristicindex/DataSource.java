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

package io.prestosql.spi.heuristicindex;

import java.io.IOException;
import java.util.Properties;

/**
 * Datasource to read splits(aka data) from. For hetu purposes its usually a queryable database/datasource (ie, Hive)
 *
 * @since 2019-08-18
 */
public interface DataSource
{
    /**
     * <pre>
     * Gets the id of the DataSource.
     *
     * This value should be the connector.name value used in the catalog files.
     * </pre>
     *
     * @return String Id
     */
    String getId();

    /**
     * <pre>
     * Reads the column values for the specified table. Filtering out partitions
     * if specified.
     *
     * For each split that is read, the Callback#call will be called, which will
     * process the read values.
     *
     * For example, for an ORC DataSource, the ORC files will be read for the
     * table and the values of the specified columns will be read. The logic for how
     * to split up the ORC files will be depend on the DataSource implementation.
     * As a simple case, each ORC file may be considered a split. For each split,
     * when the values are read, the callback is called to have the caller process
     * the read values.
     *
     * </pre>
     *
     * @param schema     schema of the table
     * @param table      table to read
     * @param columns    columns to read
     * @param partitions only read the specified partitions, set to null to read all
     *                   partitions
     * @param callback   called each time a split is read
     * @throws IOException When reading split from filesystem failed (ie, hetu does not have permission, etc)
     */
    void readSplits(String schema, String table, String[] columns, String[] partitions, Callback callback)
            throws IOException;

    /**
     * <pre>
     * These properties may be used as configs for the DataSource to connect
     * to the underlying datasource. For example, HDFS configs. Each
     * DataSource determines how to use these properties.
     * </pre>
     *
     * @return Properties of the DataSource
     */
    default Properties getProperties()
    {
        return new Properties();
    }

    /**
     * <pre>
     * Sets the properties of the DataSource.
     *
     * These properties may be used as configs for the DataSource to connect
     * to the underlying datasource. For example, HDFS configs.
     *
     * Each DataSource determines how to use these properties.
     * </pre>
     *
     * @param properties The properties to be set
     */
    default void setProperties(Properties properties)
    {
    }

    /**
     * Used by the DatSource to allow processing of splits.
     *
     * @since 2019-08-18
     */
    interface Callback
    {
        /**
         * This callback is used by the DataSource while reading the splits.
         * Each time a split is read, the DataSource will call this method
         * providing information about the split read and the data
         *
         * @param column     column that was read
         * @param values     values of the column read
         * @param uri        uri of the file or source that was read
         * @param splitStart the split offset, e.g. if the source was a large file,
         *                   it may have been read in multiple splits
         * @param progress   Value between 0.0 and 1.0 representing the progress.
         */
        void call(String column, Object[] values, String uri, long splitStart, long lastModified, double progress);
    }
}
