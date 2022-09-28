/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.shardingsphere;

import io.hetu.core.plugin.singledata.shardingsphere.props.EtcdPropertyKey;
import io.hetu.core.plugin.singledata.shardingsphere.props.ZookeeperPropertyKey;
import io.prestosql.spi.PrestoException;

import java.util.Properties;

import static io.hetu.core.plugin.singledata.SingleDataUtils.OPENGAUSS_DRIVER_NAME;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class ShardingSphereUtils
{
    public static void loadDrivers()
    {
        try {
            Class.forName(OPENGAUSS_DRIVER_NAME);
        }
        catch (ClassNotFoundException ignored) {
            throw new PrestoException(JDBC_ERROR, "Can not load openGauss JDBC driver");
        }
    }

    public static void fillProperties(Properties properties, ShardingSphereConfig config)
    {
        if (config.getType().equals("zookeeper")) {
            properties.put(ZookeeperPropertyKey.RETRY_INTERVAL_MILLISECONDS, config.getZkRetryIntervalMilliseconds());
            properties.put(ZookeeperPropertyKey.MAX_RETRIES, config.getZkMaxRetries());
            properties.put(ZookeeperPropertyKey.TIME_TO_LIVE_SECONDS, config.getZkTimeToLiveSeconds());
            properties.put(ZookeeperPropertyKey.OPERATION_TIMEOUT_MILLISECONDS, config.getZkOperationTimeoutMilliseconds());
            properties.put(ZookeeperPropertyKey.DIGEST, config.getZkDigest());
        }
        else {
            properties.put(EtcdPropertyKey.TIME_TO_LIVE_SECONDS, config.getEtcdTimeToLiveSeconds());
            properties.put(EtcdPropertyKey.CONNECTION_TIMEOUT_SECONDS, config.getEtcdConnectionTimeout());
        }
    }

    private ShardingSphereUtils() {}
}
