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

package io.hetu.core.plugin.singledata.tidrange;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class TidRangeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(TidRangeConfig.class);
        configBinder(binder).bindConfig(OpenGaussClientConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);

        binder.bind(JdbcClient.class).to(TidRangeClient.class).in(Scopes.SINGLETON);
        binder.bind(ConnectionFactory.class).to(TidRangeConnectionFactory.class).in(Scopes.SINGLETON);
    }
}
