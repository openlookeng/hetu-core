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
import React from 'react';
import {Table, Column, Cell} from 'fixed-data-table-2';
import _ from 'lodash';
import FQN from '../utils/fqn';
import TableStore from '../stores/TableStore';
import TableActions from '../actions/TableActions';
import UpdateWidthMixin from '../mixins/UpdateWidthMixin';
import ResultsPreviewStore from "../stores/ResultsPreviewStore";

let isColumnResizing = false;

// State actions
function getStateFromStore() {
  return {
    table: TableStore.getActiveTable()
  };
}

function getColumns(columns, widths, rowGetter) {
  return columns.map(function(column, i) {
    return (
        <Column
            header={<Cell>{column.name}</Cell>}
            width={widths[i]}
            dataKey={i}
            columnKey={i}
            key={i}
            isResizable={true}
            // cell={cellRenderer}
            cell={({rowIndex}) => <Cell className="text-overflow-ellipsis">{rowGetter(rowIndex)[i]}</Cell>}
            minWidth={80}
        />);
  });
}

class DataPreview
    extends React.Component {

  constructor(props) {
    super(props);
    this.state = getStateFromStore();
    this._renderEmptyMessage = this._renderEmptyMessage.bind(this);
    this._renderColumns = this._renderColumns.bind(this);
    this.rowGetter = this.rowGetter.bind(this);
    this._enhancedColumns = this._enhancedColumns.bind(this);
    this._enhancedData = this._enhancedData.bind(this);
    this._onChange = this._onChange.bind(this);
    this._onColumnResizeEndCallback = this._onColumnResizeEndCallback.bind(this);
  }

  componentDidMount() {
    TableStore.listen(this._onChange);
  }

  componentWillUnmount() {
    TableStore.unlisten(this._onChange);
  }

  render() {
    if (this.state.table && this.state.table.data) {
      return this._renderColumns();
    } else {
      return this._renderEmptyMessage();
    }
  }

  /* Internal Helpers ------------------------------------------------------- */
  _renderEmptyMessage() {
    return (
      <div className="panel-body text-light text-center">
        <p>Please select a table.</p>
      </div>
    )
  }

  _renderColumns() {
    return (
      <div className='flex flex-column hetu-table'>
        <div className='editor-menu'>
          <strong>{this.state.table && this.state.table.name}</strong>
        </div>
        <Table
          headerHeight={25}
          rowHeight={35}
          rowsCount={this.state.table.data.length}
          width={this.props.tableWidth}
          maxHeight={this.props.tableHeight - 39}
          isResizable={isColumnResizing}
          onColumnResizeEndCallback={this._onColumnResizeEndCallback}
          {... this.props}>
          {getColumns(this.state.table.columns, this.state.table.columnWidths, this.rowGetter)}
        </Table>
      </div>
    );
  }

  rowGetter(rowIndex) {
    return this.state.table.data[rowIndex];
  }

  _enhancedColumns() {
    return _.map(this.state.table.columns, function(column) {
      return column.name;
    });
  }

  _enhancedData() {
    return _.map(this.state.table.data, function(item) {
      return _.transform(item, function(result, n, key) {
        let text = _.isBoolean(n) ? n.toString() : n;
        result[this.state.table.columns[key].name] = text;
      }.bind(this));
    }.bind(this));
  }

  /* Store events */
  _onChange() {
    this.setState(getStateFromStore());
  }

  _onColumnResizeEndCallback(newColumnWidth, dataKey) {
    isColumnResizing = false;
    TableActions.setTableColumnWidth(dataKey, newColumnWidth);
  }
}

export default DataPreview;
