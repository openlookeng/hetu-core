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

package io.hetu.core.plugin.heuristicindex.datasource.hive;

import io.prestosql.spi.heuristicindex.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * DataSource implementation for reading table data from hive.
 * Currently only support reading ORC files. Parquet support will be added later.
 *
 * @since 2019-09-25
 */
public class HiveDataSource
        implements DataSource
{
    private static final Logger LOG = LoggerFactory.getLogger(HiveDataSource.class);

    private static final String ID = "hive-hadoop2";

    private DataSource delegate;
    private Properties properties;

    /**
     * Empty default constructor
     */
    public HiveDataSource()
    {
    }

    /**
     * Construct the HiveDataSource given the data source specific properties
     *
     * @param properties properties extracted from configuration files for this data source
     */
    public HiveDataSource(Properties properties)
    {
        setProperties(properties);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public void readSplits(
            String database,
            String table,
            String[] columns,
            String[] partitions,
            Callback callback) throws IOException
    {
        if (delegate == null) {
            delegate = getDelegate(database, table);
        }

        delegate.readSplits(database, table, columns, partitions, callback);
    }

    private DataSource getDelegate(String databaseName, String tableName)
    {
        TableMetadata tableMetadata = HadoopUtil.getTableMetadata(databaseName, tableName, getProperties());
        requireNonNull(tableMetadata, "unable to get table metadata");

        if (HdfsOrcDataSource.isOrcTable(tableMetadata)) {
            return new HdfsOrcDataSource(getProperties(), null, null);
        }
        else {
            String msg = String.format("The Hive table has an unsupported storage type. Supported types include: %s",
                    new HdfsOrcDataSource().getId());
            throw new IllegalArgumentException(msg);
        }
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
