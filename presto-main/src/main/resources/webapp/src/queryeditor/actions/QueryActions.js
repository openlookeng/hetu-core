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
import alt from '../alt';
import QueryApiUtils from '../utils/QueryApiUtils'
import logError from '../utils/logError'

class QueryActions {
  constructor() {
    this.generateActions(
      'receivedQuery',
      'receivedQueries',
      'selectQuery',
      'setSessionContext'
    );
  }

  createQuery(data) {
    QueryApiUtils.createQuery(data).then((query) => {
      this.actions.receivedQuery(query);
    }).catch(logError);
  }

  destroyQuery(uuid) {
    QueryApiUtils.destroyQuery(uuid).then(() => {
      this.dispatch(uuid);
    }).catch(logError);
  }

  fetchSavedQueries() {
    QueryApiUtils.fetchSavedQueries().then((results) => {
      this.actions.receivedQueries(results);
    }).catch(logError);
  }
}

export default alt.createActions(QueryActions);
