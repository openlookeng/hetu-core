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

package io.hetu.core.materializedview.metadata;

import com.google.inject.Inject;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.type.TypeManager;

/**
 * Mv Connector metadata factory
 *
 * @since 2020-03-27
 */
public class MaterializedViewConnectorMetadataFactory
{
    private final MaterializedViewMetastoreFactory metastoreFactory;
    private final TypeManager typeManager;
    private final HetuMetastore hetuMetastore;

    @Inject
    public MaterializedViewConnectorMetadataFactory(MaterializedViewMetastoreFactory metastoreFactory, TypeManager typeManager, HetuMetastore hetuMetastore)
    {
        this.metastoreFactory = metastoreFactory;
        this.typeManager = typeManager;
        this.hetuMetastore = hetuMetastore;
    }

    /**
     * create mv connector metadata
     *
     * @return MvConnectorMetadata
     */
    public MaterializedViewConnectorMetadata create()
    {
        return new MaterializedViewConnectorMetadata(metastoreFactory, typeManager, hetuMetastore);
    }
}
