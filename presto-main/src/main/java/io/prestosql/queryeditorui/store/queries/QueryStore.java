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

import io.prestosql.queryeditorui.protocol.queries.SavedQuery;
import io.prestosql.queryeditorui.protocol.queries.UserSavedQuery;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface QueryStore
{
    List<SavedQuery> getSavedQueries(Optional<String> user);

    boolean saveQuery(UserSavedQuery query) throws IOException;

    boolean deleteSavedQuery(String user, UUID queryUUID);

    SavedQuery getSavedQuery(UUID queryUUID);
}
