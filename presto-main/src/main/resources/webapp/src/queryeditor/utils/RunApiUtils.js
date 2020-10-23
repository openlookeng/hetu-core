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
import checkResults from './checkResults';
import xhr from './xhr';

export default {
  execute(query) {
    return xhr('../api/execute', {
      method: 'put',
      body: JSON.stringify({
        query:query.query,
        sessionContext: query.sessionContext
      })
    });
  },

  fetchForUser(user) {
    return xhr(`../api/users/${user.name}/queries`).then(checkResults);
  },

  fetchHistory() {
    return xhr('../api/query/history').then(checkResults);
  },

  kill(uuid) {
    return xhr(`../api/queries/${uuid}`, {
      method: 'delete'
    });
  }
};
