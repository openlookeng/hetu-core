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
package io.prestosql.queryeditorui.store.queries;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.queryeditorui.protocol.queries.SavedQuery;
import io.prestosql.queryeditorui.protocol.queries.UserSavedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class InMemoryQueryStore
        implements QueryStore
{
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryQueryStore.class);

    static final JsonCodec<List<UserSavedQuery>> SAVED_QUERIES_CODEC =
            new JsonCodecFactory(new ObjectMapperProvider()).listJsonCodec(UserSavedQuery.class);
    private List<UserSavedQuery> featuredQueries = new ArrayList<>();
    private List<UserSavedQuery> userQueries = new ArrayList<>();
    private File featuredQueriesPath;
    private File userQueriesPath;

    public InMemoryQueryStore(File featuredQueriesPath, File userQueriesPath) throws IOException
    {
        loadFeaturedQueries(featuredQueriesPath);
        this.featuredQueriesPath = featuredQueriesPath;
        this.userQueriesPath = userQueriesPath;
    }

    private synchronized void loadFeaturedQueries(File featuredQueriesPath) throws IOException
    {
        if (!featuredQueriesPath.exists()) {
            return;
        }
        try (FileInputStream fin = new FileInputStream(featuredQueriesPath)) {
            StringBuilder jsonString = new StringBuilder();
            byte[] buffer = new byte[4096];
            int n;
            while ((n = fin.read(buffer)) > 0) {
                jsonString.append(new String(buffer, 0, n));
            }
            featuredQueries.addAll(SAVED_QUERIES_CODEC.fromJson(jsonString.toString()));
            featuredQueries.stream().forEach(q -> q.setFeatured(true));
        }
        catch (IOException e) {
            LOG.error("Featured queries file not found", e);
            throw e;
        }
    }

    @Override
    public List<SavedQuery> getSavedQueries(Optional<String> user)
    {
        if (user.isPresent()) {
            return featuredQueries.stream()
                    .filter(q -> q.getUser().equals(user.get()))
                    .collect(Collectors.toList());
        }
        return featuredQueries.stream().collect(Collectors.toList());
    }

    @Override
    public synchronized boolean saveQuery(UserSavedQuery query) throws IOException
    {
        List<UserSavedQuery> queryList;
        File targetPath;
        if (query.isFeatured()) {
            queryList = featuredQueries;
            targetPath = featuredQueriesPath;
        }
        else {
            queryList = userQueries;
            targetPath = userQueriesPath;
        }
        queryList.add(query);
        try (FileOutputStream fout = new FileOutputStream(targetPath)) {
            String json = SAVED_QUERIES_CODEC.toJson(queryList);
            fout.write(json.getBytes());
        }
        catch (IOException e) {
            LOG.error("Error while saving queries", e);
            throw e;
        }
        return true;
    }

    @Override
    public boolean deleteSavedQuery(String user, UUID queryUUID)
    {
        for (Iterator<UserSavedQuery> it = userQueries.iterator(); it.hasNext(); ) {
            UserSavedQuery query = it.next();
            if (query.getUser().equalsIgnoreCase(user) && query.getUuid().equals(queryUUID)) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    @Override
    public SavedQuery getSavedQuery(UUID queryUUID)
    {
        UserSavedQuery query = featuredQueries.stream()
                .filter(q -> q.getUuid().equals(queryUUID))
                .findFirst().orElse(null);
        if (query != null) {
            return query;
        }
        return userQueries.stream()
                .filter(q -> q.getUuid().equals(queryUUID))
                .findFirst().orElse(null);
    }
}
