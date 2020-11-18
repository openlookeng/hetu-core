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
package io.hetu.core.plugin.functionnamespace.memory;

import io.hetu.core.plugin.functionnamespace.AbstractSqlInvokedFunctionNamespaceManager;
import io.hetu.core.plugin.functionnamespace.ServingCatalog;
import io.hetu.core.plugin.functionnamespace.SqlInvokedFunctionNamespaceManagerConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.function.SqlFunctionId;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

@ThreadSafe
public class InMemoryFunctionNamespaceManager
        extends AbstractSqlInvokedFunctionNamespaceManager
{
    private final Map<SqlFunctionId, SqlInvokedFunction> latestFunctions = new ConcurrentHashMap<>();
    private final Map<TypeSignature, Type> types = new ConcurrentHashMap<>();

    @Inject
    public InMemoryFunctionNamespaceManager(
            @ServingCatalog String catalogName,
            SqlInvokedFunctionNamespaceManagerConfig config)
    {
        super(catalogName, config);
    }

    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        // Do not need to do anything here since InMemoryFunctionNamespaceManager cannot execute functions
    }

    @Override
    public synchronized void createFunction(SqlInvokedFunction function, boolean replace)
    {
        checkFunctionLanguageSupported(function);
        SqlFunctionId functionId = function.getFunctionId();
        if (!replace && latestFunctions.containsKey(function.getFunctionId())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Function '%s' already exists", functionId.getId()));
        }

        SqlInvokedFunction replacedFunction = latestFunctions.get(functionId);
        long version = 1;
        if (replacedFunction != null) {
            version = parseLong(replacedFunction.getRequiredVersion()) + 1;
        }
        latestFunctions.put(functionId, function.withVersion(String.valueOf(version)));
        refreshFunctionsCache(function.getFunctionId().getFunctionName());
    }

    @Override
    public List<SqlInvokedFunction> listFunctions()
    {
        return new ArrayList<>(latestFunctions.values());
    }

    @Override
    public List<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName name)
    {
        return new ArrayList<>(latestFunctions.values().stream()
                .filter(function -> function.getSignature().getName().equals(name))
                .map(InMemoryFunctionNamespaceManager::copyFunction)
                .collect(toImmutableList()));
    }

    @Override
    public Optional<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName, List<TypeSignature> typeSignatureList)
    {
        Collection<SqlInvokedFunction> collection = fetchFunctionsDirect(functionName);
        for (SqlInvokedFunction sqlInvokedFunction : collection) {
            List<TypeSignature> list = sqlInvokedFunction.getFunctionId().getArgumentTypes();
            if (list.size() != typeSignatureList.size()) {
                continue;
            }
            boolean argsEqual = true;
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != typeSignatureList.get(i)) {
                    argsEqual = false;
                    break;
                }
            }
            if (argsEqual && functionName.equals(sqlInvokedFunction.getFunctionId().getFunctionName())) {
                return Optional.of(sqlInvokedFunction);
            }
        }
        return Optional.empty();
    }

    @Override
    public FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToMetadata)
                .collect(onlyElement());
    }

    @Override
    protected ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle)
    {
        return fetchFunctionsDirect(functionHandle.getFunctionId().getFunctionName()).stream()
                .filter(function -> function.getRequiredFunctionHandle().equals(functionHandle))
                .map(this::sqlInvokedFunctionToImplementation)
                .collect(onlyElement());
    }

    private static long getLongVersion(SqlInvokedFunction function)
    {
        return parseLong(function.getVersion().get());
    }

    private static SqlInvokedFunction copyFunction(SqlInvokedFunction function)
    {
        return new SqlInvokedFunction(
                function.getSignature().getName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.getDescription(),
                function.getRoutineCharacteristics(),
                function.getBody(),
                function.getFunctionProperties(),
                function.getVersion());
    }
}
