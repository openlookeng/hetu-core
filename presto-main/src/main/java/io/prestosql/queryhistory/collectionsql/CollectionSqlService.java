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
package io.prestosql.queryhistory.collectionsql;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.queryhistory.MockhetuMetaStore;
import io.prestosql.queryhistory.QueryHistoryConfig;
import io.prestosql.queryhistory.model.FavoriteInfo;
import io.prestosql.spi.favorite.FavoriteEntity;
import io.prestosql.spi.favorite.FavoriteResult;
import io.prestosql.spi.metastore.HetuMetastore;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class CollectionSqlService
{
    private static final Logger log = Logger.get(CollectionSqlService.class);
    private final HetuMetaStoreManager hetuMetaStoreManager;
    private final QueryHistoryConfig queryHistoryConfig;
    private HetuMetastore hetuMetastore;

    @Inject
    public CollectionSqlService(HetuMetaStoreManager hetuMetaStoreManager, QueryHistoryConfig queryHistoryConfig)
    {
        this.hetuMetaStoreManager = requireNonNull(hetuMetaStoreManager, "MetaStoreManager is null");
        this.queryHistoryConfig = requireNonNull(queryHistoryConfig, "queryHistoryConfig is null");
        this.hetuMetastore = validateMetaStore();
    }

    private HetuMetastore validateMetaStore()
    {
        if (hetuMetaStoreManager.getHetuMetastore() == null) {
            this.reviseHetuMetastore();
            return MockhetuMetaStore.getInstance();
        }
        return hetuMetaStoreManager.getHetuMetastore();
    }

    /**
     * reviseHetuMetastore
     */
    private void reviseHetuMetastore()
    {
        new Thread(() -> {
            while (hetuMetaStoreManager.getHetuMetastore() == null) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                }
                catch (InterruptedException e) {
                    log.error(e, "Error reviseHetuMetastore");
                }
            }

            this.hetuMetastore = validateMetaStore();
        }).start();
    }

    public Boolean insert(FavoriteInfo favoriteInfo)
    {
        String user = favoriteInfo.getUser();
        String query = favoriteInfo.getQuery();
        String catalog = favoriteInfo.getCatalog();
        String schema = favoriteInfo.getSchemata();
        if (hetuMetastore.getFavorite(1, 1, user).getTotal() >= queryHistoryConfig.getMaxCollectionSqlCount()) {
            return false;
        }
        hetuMetastore.insertFavorite(new FavoriteEntity(user, query, catalog, schema));
        return true;
    }

    public Boolean delete(FavoriteInfo favoriteInfo)
    {
        String user = favoriteInfo.getUser();
        String query = favoriteInfo.getQuery();
        String catalog = favoriteInfo.getCatalog();
        String schemata = favoriteInfo.getSchemata();
        hetuMetastore.deleteFavorite(new FavoriteEntity(user, query, catalog, schemata));
        return true;
    }

    public FavoriteResult query(int pageNum, int pageSize, String user)
    {
        int startNum = (pageNum - 1) * pageSize;
        FavoriteResult favoriteResult = hetuMetastore.getFavorite(startNum, pageSize, user);
        return favoriteResult;
    }
}
