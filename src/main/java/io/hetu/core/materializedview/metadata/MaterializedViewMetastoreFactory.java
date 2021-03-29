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

import io.hetu.core.materializedview.conf.MaterializedViewConfig;
import io.hetu.core.materializedview.utils.MaterializedViewErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.metastore.HetuMetastore;

import javax.inject.Inject;

import static io.hetu.core.materializedview.utils.MaterializedViewConstants.TYPE_HETU;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.TYPE_LOCAL;

/**
 * MvMetastoreFactory
 *
 * @since 2020-03-26
 */
public class MaterializedViewMetastoreFactory
{
    private final MaterializedViewConfig mvConfig;

    /**
     * constructor of MvMetastore factory
     *
     * @param mvConfig config
     */
    @Inject
    public MaterializedViewMetastoreFactory(MaterializedViewConfig mvConfig)
    {
        this.mvConfig = mvConfig;
    }

    /**
     * create mv metastore
     *
     * @return MvMetastore
     */
    public MaterializedViewMetastore create(HetuMetastore hetuMetastore)
    {
        String type = mvConfig.getMetastoreType();
        if (type == null || TYPE_LOCAL.equals(type)) {
            return new LocalMaterializedViewMetastore(mvConfig);
        }
        else if (TYPE_HETU.equals(type)) {
            return new HetuMaterializedViewMetastore(hetuMetastore);
        }
        else {
            throw new PrestoException(MaterializedViewErrorCode.UNSUPPORTED_TYPE, "Unsupported metastore type");
        }
    }
}
