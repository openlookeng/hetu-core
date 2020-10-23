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
import TableApiUtils from '../utils/TableApiUtils';
import logError from '../utils/logError'

class TableActions {
  constructor() {
    this.generateActions(
      'addTable',
      'removeTable',
      'selectTable',
      'unselectTable',
      'selectPartition',
      'unselectPartition'
    );
  }

  fetchTable(table) {
    // Fetch the data from the new table
    TableApiUtils.fetchTableData(table).then(
      ({table, columns, data}) => {
        this.actions.receivedTableData(table, columns, data);
      }
    ).catch(logError);
  }

  fetchTables() {
    TableApiUtils.fetchTables().then((tables) => {
      this.dispatch(tables);
    }).catch(logError);
  }

  fetchTablePreview(table, name, value) {
    TableApiUtils.fetchTablePreviewData(table, {name, value}).then(
      ({table, partition, data}) => {
        this.actions.receivedPartitionData({table, partition, data});
      }
    ).catch(logError);
  }

  setTableColumnWidth(columnIdx, width) {
    this.dispatch({ columnIdx, width });
  }

  receivedTableData(table, columns, data) {
    this.dispatch({ table, columns, data });
  }

  receivedPartitionData({table, partition, data}) {
    this.dispatch({ table, partition, data });
  }
}

export default alt.createActions(TableActions);
