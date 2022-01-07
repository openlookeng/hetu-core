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
package io.prestosql.queryhistory.collectionsql;

import com.google.inject.Inject;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.queryhistory.model.FavoriteInfo;
import io.prestosql.spi.favorite.FavoriteEntity;
import io.prestosql.spi.favorite.FavoriteResult;

import static java.util.Objects.requireNonNull;

public class CollectionSqlService
{
    private final HetuMetaStoreManager hetuMetaStoreManager;

    @Inject
    public CollectionSqlService(HetuMetaStoreManager hetuMetaStoreManager)
    {
        this.hetuMetaStoreManager = requireNonNull(hetuMetaStoreManager, "MetaStoreManager is null");
    }

    public Boolean insert(FavoriteInfo favoriteInfo)
    {
        String user = favoriteInfo.getUser();
        String query = favoriteInfo.getQuery();
        String catalog = favoriteInfo.getCatalog();
        String schema = favoriteInfo.getSchemata();
        try {
            hetuMetaStoreManager.getHetuMetastore().insertFavorite(new FavoriteEntity(user, query, catalog, schema));
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Boolean delete(FavoriteInfo favoriteInfo)
    {
        String user = favoriteInfo.getUser();
        String query = favoriteInfo.getQuery();
        String catalog = favoriteInfo.getCatalog();
        String schemata = favoriteInfo.getSchemata();
        try {
            hetuMetaStoreManager.getHetuMetastore().deleteFavorite(new FavoriteEntity(user, query, catalog, schemata));
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public FavoriteResult query(int pageNum, int pageSize, String user)
    {
        int startNum = (pageNum - 1) * pageSize;
        FavoriteResult favoriteResult = hetuMetaStoreManager.getHetuMetastore().getFavorite(startNum, pageSize, user);
        return favoriteResult;
    }
}
