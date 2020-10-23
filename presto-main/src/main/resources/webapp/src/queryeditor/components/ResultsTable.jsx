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
import {Cell, Column, Table} from 'fixed-data-table-2';
import _ from 'lodash';
import ResultsPreviewStore from '../stores/ResultsPreviewStore';
import ResultsPreviewActions from '../actions/ResultsPreviewActions';
import QueryActions from '../actions/QueryActions';

let isColumnResizing = false;

// State actions
function getStateFromStore() {
  return {
    query: ResultsPreviewStore.getPreviewQuery(),
    table: ResultsPreviewStore.getResultsPreview()
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

function selectQuery(query, e) {
  e.preventDefault();
  QueryActions.selectQuery(query);
}

class ResultsTable
    extends React.Component {
  constructor(props) {
    super(props);
    this.state = getStateFromStore();
    this._onChange = this._onChange.bind(this);
    this._enhancedData = this._enhancedData.bind(this);
    this.rowGetter = this.rowGetter.bind(this);
    this._enhancedColumns = this._enhancedColumns.bind(this);
    this._renderColumns = this._renderColumns.bind(this);
  }

  componentDidMount() {
    ResultsPreviewStore.listen(this._onChange);
  }

  componentWillUnmount() {
    ResultsPreviewStore.unlisten(this._onChange);
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
        <p>Please make a query.</p>
      </div>)
  }

  _renderColumns() {
    return (
      <div className='flex flex-column hetu-table'>
        <div className='editor-menu'>
          <div 
            style={{width: this.props.tableWidth - 20}}
            className="text-overflow-ellipsis">
            <a href="#" onClick={selectQuery.bind(null, this.state.query)}>
              {this.state.query.query}
            </a>
          </div>
        </div>
        <Table
          headerHeight={25}
          rowHeight={40}
          rowsCount={this.state.table.data.length}
          width={this.props.tableWidth}
          height={this.props.tableHeight - 39}
          maxHeight={this.props.tableHeight - 39}
          isResizable={isColumnResizing}
          onColumnResizeEndCallback={this._onColumnResizeEndCallback}
          {... this.props}>
          {getColumns(this.state.table.columns, this.state.table.columnWidths, this.rowGetter)}
        </Table>
      </div>);
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
    ResultsPreviewActions.setTableColumnWidth(dataKey, newColumnWidth);
  }
}

export default ResultsTable;
