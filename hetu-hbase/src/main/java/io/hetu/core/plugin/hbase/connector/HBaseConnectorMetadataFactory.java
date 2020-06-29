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
package io.hetu.core.plugin.hbase.connector;

import com.google.inject.Inject;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.metadata.HBaseConnectorMetadata;

/**
 * HBaseConnectorMetadataFactory
 *
 * @since 2020-03-30
 */
public class HBaseConnectorMetadataFactory
{
    private final HBaseConnection hbaseConnection;
    private final HBaseConfig hbaseConfig;

    /**
     * constructor
     *
     * @param hbaseConnection hbaseConnection
     * @param config config
     */
    @Inject
    public HBaseConnectorMetadataFactory(HBaseConnection hbaseConnection, HBaseConfig config)
    {
        this.hbaseConnection = hbaseConnection;
        this.hbaseConfig = config;
    }

    /**
     * create
     *
     * @return HBaseConnectorMetadata
     */
    public HBaseConnectorMetadata create()
    {
        return new HBaseConnectorMetadata(hbaseConnection);
    }
}
