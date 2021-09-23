/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.datacenter;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.hetu.core.plugin.datacenter.optimization.DataCenterPlanOptimizer;
import io.hetu.core.plugin.datacenter.optimization.DataCenterQueryGenerator;
import io.prestosql.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Data Center Module
 *
 * @since 2020-02-11
 */
public class DataCenterModule
        implements Module
{
    private final TypeManager typeManager;

    /**
     * The constructor of data center module.
     *
     * @param typeManager the type manager of data center.
     */
    public DataCenterModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(DataCenterConnector.class).in(Scopes.SINGLETON);
        binder.bind(DataCenterPlanOptimizer.class).in(Scopes.SINGLETON);
        binder.bind(DataCenterQueryGenerator.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DataCenterConfig.class);
    }
}
