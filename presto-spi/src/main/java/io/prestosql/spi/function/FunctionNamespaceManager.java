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
package io.prestosql.spi.function;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface FunctionNamespaceManager<F extends SqlFunction>
{
    /**
     * Start a transaction.
     */
    FunctionNamespaceTransactionHandle beginTransaction();

    /**
     * Commit the transaction. Will be called at most once and will not be called if
     * {@link #abort(FunctionNamespaceTransactionHandle)} is called.
     */
    void commit(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Rollback the transaction. Will be called at most once and will not be called if
     * {@link #commit(FunctionNamespaceTransactionHandle)} is called.
     */
    void abort(FunctionNamespaceTransactionHandle transactionHandle);

    /**
     * Create or replace the specified function.
     */
    void createFunction(SqlInvokedFunction function, boolean replace);

    /**
     * List all functions managed by the {@link FunctionNamespaceManager}.
     */
    List<F> listFunctions();

    List<F> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName);

    FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature);

    FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle);

    ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle);

    default CompletableFuture<Block> executeFunction(FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<Block> executeRemoteFunction(FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager, ConnectorSession connectorSession)
    {
        throw new UnsupportedOperationException();
    }
}
