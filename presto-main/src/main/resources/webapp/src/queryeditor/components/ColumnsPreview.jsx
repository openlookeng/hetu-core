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
import Column from './Column';
import _ from 'lodash';
import TableStore from '../stores/TableStore';
import FQN from '../utils/fqn';

// State actions
function getStateFromStore() {
  return {
    table: TableStore.getActiveTable()
  };
}

class ColumnsPreview
    extends React.Component{
  constructor(props) {
    super(props);
    this.state = getStateFromStore();
    this._renderColumns = this._renderColumns.bind(this);
    this._onChange = this._onChange.bind(this);
  }


  componentDidMount() {
    TableStore.listen(this._onChange);
  }

  componentWillUnmount() {
    TableStore.unlisten(this._onChange);
  }

  render() {
    if( this.state.table && this.state.table.columns ) {
      return this._renderColumns(this.state.table.columns);
    } else {
      return this._renderEmptyMessage();
    }
  }

  /* Internal Helpers ------------------------------------------------------- */
  _renderColumns(collection) {
    let columns;

    let normalCols = _.chain(collection).sortBy('name').value();

    columns = _.chain(normalCols).reduce(function(m, col) {
      let reuseGroup = (m.length > 0) && (m[m.length - 1].length < 1), group = reuseGroup ? m[m.length - 1] : [], val;

      group.push(
        <Column
          key={col.name}
          name={col.name}
          type={col.type}
          partition={col.partition} />
      );

      if (!reuseGroup) {
        m.push(group);
      }

      return m;
    }, []).map(function(col, i) {
      return (
        <div key={'col-row-'+i}>
          {col}
        </div>
      );
    }).value();

    // Render the template
    return (
      <div className="flex flex-column columns-container panel-body content">
        <div className='columns-label'>
          <span>
            Catalog : {FQN.catalog(this.state.table.name)}
            <br/>
            Schema : {FQN.simpleSchema(this.state.table.name)}
            <br/>
            Table : {FQN.table(this.state.table.name)}
          </span>
        </div>
        <div style={{fontSize:"14px", fontWeight:500, color:"#020202", borderBottom: "2px solid #d6d9dc"}}>
          <Column
              key={"column_preview_header"}
              name={"Column Name"}
              type={"Data Type"}
              header={true}/>
        </div>
        <div className='scroll-container'>
          {columns}
        </div>
      </div>
    );
  }

  _renderEmptyMessage() {
    return (
      <div className="flex justify-center text-light panel-body text-center">
        <p>Please select a table.</p>
      </div>
    )
  }

  _capitalize(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
  }

  /* Store events */
  _onChange() {
    this.setState(getStateFromStore());
  }
}

export default ColumnsPreview;
