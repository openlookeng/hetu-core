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
package io.prestosql.metadata;

import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.type.TypeManager;

import java.util.Optional;

public class FunctionNamespaceManagerContextInstance
        implements FunctionNamespaceManagerContext
{
    private final HetuMetastore hetuMetastore;

    private final TypeManager typeManager;

    public FunctionNamespaceManagerContextInstance(HetuMetastore hetuMetastore, TypeManager typeManager)
    {
        this.hetuMetastore = hetuMetastore;
        this.typeManager = typeManager;
    }

    @Override
    public Optional<HetuMetastore> getHetuMetastore()
    {
        if (hetuMetastore == null) {
            return Optional.empty();
        }
        return Optional.of(hetuMetastore);
    }

    @Override
    public Optional<TypeManager> getTypeManager()
    {
        if (typeManager == null) {
            return Optional.empty();
        }
        return Optional.of(typeManager);
    }
}
