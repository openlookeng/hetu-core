/*
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
package io.prestosql.spi;

import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.cube.CubeProvider;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.filesystem.HetuFileSystemClientFactory;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.metastore.HetuMetaStoreFactory;
import io.prestosql.spi.queryeditorui.ConnectorWithProperties;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.prestosql.spi.security.GroupProviderFactory;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.spi.session.SessionPropertyConfigurationManagerFactory;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Plugin
{
    default Iterable<ConnectorFactory> getConnectorFactories()
    {
        return emptyList();
    }

    default Iterable<BlockEncoding> getBlockEncodings()
    {
        return emptyList();
    }

    default Iterable<Type> getTypes()
    {
        return emptyList();
    }

    default Iterable<ParametricType> getParametricTypes()
    {
        return emptyList();
    }

    default Set<Class<?>> getFunctions()
    {
        return emptySet();
    }

    default Set<Object> getDynamicHiveFunctions()
    {
        return emptySet();
    }

    default Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        return emptyList();
    }

    default Iterable<GroupProviderFactory> getGroupProviderFactories()
    {
        return emptyList();
    }

    default Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return emptyList();
    }

    default Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return emptyList();
    }

    default Iterable<ResourceGroupConfigurationManagerFactory> getResourceGroupConfigurationManagerFactories()
    {
        return emptyList();
    }

    default Iterable<SessionPropertyConfigurationManagerFactory> getSessionPropertyConfigurationManagerFactories()
    {
        return emptyList();
    }

    default Iterable<StateStoreFactory> getStateStoreFactories()
    {
        return emptyList();
    }

    default Iterable<StateStoreBootstrapper> getStateStoreBootstrappers()
    {
        return emptyList();
    }

    default Iterable<SeedStoreFactory> getSeedStoreFactories()
    {
        return emptyList();
    }

    default Iterable<CubeProvider> getCubeProviders()
    {
        return emptyList();
    }

    default Iterable<HetuFileSystemClientFactory> getFileSystemClientFactory()
    {
        return emptyList();
    }

    default Iterable<HetuMetaStoreFactory> getHetuMetaStoreFactories()
    {
        return emptyList();
    }

    default Iterable<IndexFactory> getIndexFactories()
    {
        return emptyList();
    }

    default Optional<ConnectorWithProperties> getConnectorWithProperties()
    {
        return Optional.empty();
    }

    default void setExternalFunctionsDir(File externalFuncsDir)
    {}

    default void setMaxFunctionRunningTimeEnable(boolean enable)
    {}

    default void setMaxFunctionRunningTimeInSec(long time)
    {}

    default void setFunctionRunningThreadPoolSize(int size)
    {}

    default Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return emptyList();
    }
}
