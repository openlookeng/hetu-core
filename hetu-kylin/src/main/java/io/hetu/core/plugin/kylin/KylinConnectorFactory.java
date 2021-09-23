/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;

import java.sql.Driver;
import java.util.Optional;
import java.util.Properties;

public class KylinConnectorFactory
        extends DriverConnectionFactory
{
    public KylinConnectorFactory(Driver driver,
            String connectionUrl,
            Optional<String> userCredentialName,
            Optional<String> passwordCredentialName,
            Properties connectionProperties,
            BaseJdbcConfig config)
    {
        super(driver,
                connectionUrl,
                userCredentialName,
                passwordCredentialName,
                connectionProperties);
    }
}
