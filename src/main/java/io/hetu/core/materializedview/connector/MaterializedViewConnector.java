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

package io.hetu.core.materializedview.connector;

import com.google.inject.Inject;
import io.hetu.core.materializedview.metadata.MaterializedViewConnectorMetadata;
import io.hetu.core.materializedview.metadata.MaterializedViewConnectorMetadataFactory;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;

/**
 * MvConnector
 *
 * @since 2020-03-28
 */
public class MaterializedViewConnector
        implements Connector
{
    private final MaterializedViewConnectorMetadata mvConnectorMetadata;

    /**
     * Constructor of mv connector
     *
     * @param mvConnectorMetadataFactory metadata connector
     */
    @Inject
    public MaterializedViewConnector(MaterializedViewConnectorMetadataFactory mvConnectorMetadataFactory)
    {
        this.mvConnectorMetadata = mvConnectorMetadataFactory.create();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return MaterializedViewTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return mvConnectorMetadata;
    }
}
