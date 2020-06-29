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
package io.prestosql.discovery.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.prestosql.discovery.HetuDiscoveryModule;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.InternalCommunicationConfig;
import io.prestosql.statestore.LocalStateStoreProvider;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import static org.testng.Assert.assertNotNull;

public class TestHetuEmbeddedDiscoveryModule
{
    @Test
    public void testBinding()
    {
        ConfigurationFactory factory = new ConfigurationFactory(ImmutableMap.<String, String>builder()
                .put("node.internal-address", "127.0.0.1")
                .put("node.environment", "environment")
                .put("http-server.http.port", "8099")
                .put("http-server.http.enabled", "true")
                .build());

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new MBeanModule())
                .add(new TestingNodeModule("environment"))
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new TestingJmxModule())
                .add(Modules.override(new DiscoveryModule()).with(new HetuDiscoveryModule()))
                .add(Modules.override(new EmbeddedDiscoveryModule()).with(new HetuEmbeddedDiscoveryModule()))
                .add(new ConfigurationModule(factory))
                .add(binder -> {
                    binder.bind(InternalCommunicationConfig.class);
                    binder.bind(HetuConfig.class);
                    binder.bind(FileSystemClientManager.class);
                    binder.bind(StateStoreProvider.class).to(LocalStateStoreProvider.class);
                    binder.bind(SeedStoreManager.class);
                });
        Injector injector = Guice.createInjector(modules.build());

        assertNotNull(injector.getInstance(ServiceSelector.class));
    }
}
