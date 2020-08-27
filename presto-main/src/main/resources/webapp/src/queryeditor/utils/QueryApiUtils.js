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

let QueryApiUtils = {
  fetchSavedQueries() {
    return xhr('../api/query/saved').then(checkResults);
  },

  createQuery(data) {
    let formData = Object.keys(data).reduce((encoded, key) => {
      const encKey = encodeURIComponent(key);
      const encData = encodeURIComponent(data[key]);
      return `${encoded}&${encKey}=${encData}`
    }, '');

    return xhr('../api/query/saved', {
      method: 'post',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: formData
    }).then((uuid) => {
      return {
        uuid,
        name: data.name,
        description: data.description,

        queryWithPlaceholders: {
          query: data.query
        }
      };
    });
  },

  destroyQuery(uuid) {
    return xhr(`../api/query/saved/${uuid}`, {
      method: 'delete'
    });
  }
};

export default QueryApiUtils;
