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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.metadata.AbstractMockMetadata;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.tree.QualifiedName;

import java.util.List;
import java.util.Optional;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;

public class MockMetadata
        extends AbstractMockMetadata
{
    private final Metadata delegate = createTestMetadataManager();

    @Override
    public Type getType(TypeSignature signature)
    {
        return delegate.getType(signature);
    }

    @Override
    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        return ((BuiltInFunctionHandle) delegate.getFunctionAndTypeManager().resolveFunction(Optional.empty(), QualifiedObjectName.valueOf(name.toString()), parameterTypes)).getSignature();
    }

    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return delegate.getFunctionAndTypeManager();
    }
}
