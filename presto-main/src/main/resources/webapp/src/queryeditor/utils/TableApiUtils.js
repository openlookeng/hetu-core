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
import xhr from './xhr';

const fetchColumData = (table) => xhr(`/api/metadata/columns/${table.url}`);

const fetchPreviewData = (table, partition = {}) => {
  let url = `/api/metadata/preview/${table.url}`;
  if (partition.name && partition.value) {
    url += '?' +
      `partitionName=${partition.name}&` +
      `partitionValue=${partition.value}`;
  }

  return xhr(url);
};

const fetchPartitionData = (table) => xhr(`/api/metadata/partitions/${table.url}`);

export default {
  fetchTableData(table) {
    return Promise.all([
      fetchColumData(table),
      fetchPreviewData(table)
    ]).then(([columns, data]) => {
      return { table, columns, data };
    });
  },

  fetchTablePreviewData(table, partition) {
    return fetchPreviewData(table, partition).then((data) => {
      return { table, partition, data };
    });
  },

  fetchTables() {
    return xhr('../api/metadata/tables');
  }
};
