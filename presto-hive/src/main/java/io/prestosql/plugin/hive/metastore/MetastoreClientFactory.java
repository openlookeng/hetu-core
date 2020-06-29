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
package io.prestosql.plugin.hive.metastore;

import com.google.common.net.HostAndPort;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import org.apache.thrift.transport.TTransportException;

/**
 * Thrift HiveMetastore for FusionInsight
 *
 * @since 2020-03-10
 */
public interface MetastoreClientFactory
{
    /**
     * to create thrift metastore client
     *
     * @param address thrift hosts and ports
     * @return ThriftMetastoreClient client
     * @throws TTransportException transport exception
     */
    ThriftMetastoreClient create(HostAndPort address) throws TTransportException;
}
