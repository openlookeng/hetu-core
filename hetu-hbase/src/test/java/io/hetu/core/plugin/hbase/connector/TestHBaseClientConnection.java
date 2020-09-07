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

import io.hetu.core.plugin.hbase.client.TestHBaseConnection;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.metadata.HBaseMetastore;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * TestHBaseClientConnection
 *
 * @since 2020-03-20
 */
public class TestHBaseClientConnection
        extends HBaseConnection
{
    public TestHBaseClientConnection(HBaseConfig conf, HBaseMetastore metastore)
    {
        super(metastore, conf);
    }

    /**
     * init
     */
    public void init()
            throws IOException
    {
        this.conn = new TestHBaseConnection();
        if (conn.getAdmin() instanceof HBaseAdmin) {
            this.hbaseAdmin = (HBaseAdmin) conn.getAdmin();
        }
    }
}
