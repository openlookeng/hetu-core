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
import alt from '../alt'
import FluxCollection from '../utils/FluxCollection'
import QueryActions from '../actions/QueryActions'

class QueryStore {
  constructor() {
    // handle store listeners
    this.bindListeners({
      onSelectQuery: QueryActions.SELECT_QUERY,
      onSetSessionContext: QueryActions.SET_SESSION_CONTEXT,
      onReceivedQueries: QueryActions.RECEIVED_QUERIES,
      onReceivedQuery: QueryActions.RECEIVED_QUERY,
      onDestroyQuery: QueryActions.DESTROY_QUERY,
    });

    // export methods we can use
    this.exportPublicMethods({
      getSelectedQuery: this.getSelectedQuery,
      getCollection: this.getCollection,
      getSessionContext: this.getSessionContext
    });

    // state
    this.selectedQuery = null;
    this.sessionContext = null;
    this.collection = new FluxCollection({
      comparator: (model, index) => -1 * index
    });
  }

  onSelectQuery(queryWithContext) {
    this.selectedQuery = queryWithContext.query;
    this.sessionContext = queryWithContext.sessionContext;
  }

  onSetSessionContext(context) {
    this.sessionContext = context;
  }

  onReceivedQuery(query) {
    this.collection.add(query);
  }

  onReceivedQueries(queries) {
    this.collection.add(queries);
  }

  onDestroyQuery(uuid) {
    this.collection.remove(uuid);
  }

  getSelectedQuery() {
    return this.getState().selectedQuery;
  }

  getCollection() {
    return this.getState().collection;
  }

  getSessionContext() {
    return this.getState().sessionContext;
  }
}

export default alt.createStore(QueryStore, 'QueryStore');
