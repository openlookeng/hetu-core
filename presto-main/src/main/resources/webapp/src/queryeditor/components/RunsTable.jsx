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
import Moment from 'react-moment';
import 'moment-timezone';
import QueryActions from '../actions/QueryActions';
import ResultsPreviewActions from '../actions/ResultsPreviewActions';
import RunActions from '../actions/RunActions';
import TabActions from '../actions/TabActions';
import TableActions from '../actions/TableActions';
import TabConstants from '../constants/TabConstants';
import RunStateConstants from '../constants/RunStateConstants';
import RunStore from '../stores/RunStore';
import {Cell, Column, Table} from 'fixed-data-table-2';
import ProgressBar from "./ProgressBar";
import ModalDialog from "./ModalDialog";
import UserStore from "../stores/UserStore";

var classNames = require('classnames');
let isColumnResizing = false;
let columnWidths = {
  user: 90,
  context: 150,
  query: 460,
  status: 90,
  started: 180,
  duration: 80,
  output: 150,
};

// State actions
function getRuns(user) {
  if (user) {
    return RunStore.getCollection().where({
      user
    }, {
      sort: true
    })
  } else {
    return RunStore.getCollection().all({
      sort: true
    });
  }
}

class RunsTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      currentErrorMessage: null,
      showModal: false,
      runs: getRuns(this.props.user),
      user: UserStore.getCurrentUser()
    };
    this.getStateFromStore = this.getStateFromStore.bind(this);
    this._onChange = this._onChange.bind(this);
    this.onColumnResizeEndCallback = this.onColumnResizeEndCallback.bind(this);
    this.renderEmptyMessage = this.renderEmptyMessage.bind(this);
    this.renderModalDialog = this.renderModalDialog.bind(this);
    this.renderErrorDialogFooter = this.renderErrorDialogFooter.bind(this);
    this.rowGetter = this.rowGetter.bind(this);
    this.columnGetter = this.columnGetter.bind(this);
  }

  getStateFromStore() {
    return {
      currentErrorMessage: this.state.currentErrorMessage,
      showModal: this.state.showModal,
      runs: getRuns(this.props.user),
      user: UserStore.getCurrentUser()
    };
  }

  componentDidMount() {
    UserStore.listen(this._onChange);
    RunStore.listen(this._onChange);
  }

  componentWillUnmount() {
    RunStore.unlisten(this._onChange);
  }

  renderErrorDialogFooter() {
    return (
        <div className={"catalog-btn-part"}>
          <button className={"btn btn-success active"} style={{padding:"6px 12px", color: "#ffffff"}} onClick={toggleModal.bind(null, this, null)}>
            Close
          </button>
        </div>
    );
  }

  renderModalDialog() {
    if (!this.state.showModal) {
      return null;
    }
    return (
        <ModalDialog onClose={toggleModal.bind(null, this, null)} header={<div style={{color: "#ff0000"}}>Query Error</div>}
                     footer={this.renderErrorDialogFooter()}
                     show={this.state.showModal}>
          <div style={{backgroundColor:'#f5f5f5',border:'1px solid #ccc',borderRadius:'4px',color:'#333',padding:"15px", margin: "30px",maxHeight:'300px',overflow:'auto'}}>
            {/*<pre style={{textAlign: "left", paddingTop: "15px", margin: "30px"}}>*/}
              {this.state.currentErrorMessage}
            {/*</pre>*/}
          </div>
        </ModalDialog>
    );
  }

  render() {
    if (this.state.runs.length === 0) {
      return this.renderEmptyMessage();
    }

//    Need to make sure to wrap `Table` in a parent element so we can
//    compute the natural width of the component.
    return (
        <div className='flex hetu-table'>
          {this.renderModalDialog()}
          <Table
              rowHeight={50}
              rowsCount={this.state.runs.length}
              width={this.props.tableWidth}
              height={this.props.tableHeight}
              headerHeight={30}>
            <Column
                header={<Cell>User</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('user', rowIndex)}</Cell>}
                width={columnWidths.user}
            />
            <Column
                header={<Cell>Context</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('context', rowIndex)}</Cell>}
                width={columnWidths.context}
            />
            <Column
                header={<Cell>Query</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('query', rowIndex)}</Cell>}
                width={columnWidths.query}
            />
            <Column
                header={<Cell>Status</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('status', rowIndex)}</Cell>}
                width={columnWidths.status}
            />
            <Column
                header={<Cell>Started</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('started', rowIndex)}</Cell>}
                width={columnWidths.started}
            />
            <Column
                header={<Cell>Duration</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('duration', rowIndex)}</Cell>}
                width={columnWidths.duration}
            />
            <Column
                header={<Cell>Output</Cell>}
                cell={({rowIndex}) => <Cell> {this.columnGetter('output', rowIndex)}</Cell>}
                width={columnWidths.output}
            />
          </Table>
        </div>);
  }

  onColumnResizeEndCallback(newColumnWidth, dataKey) {
    columnWidths[dataKey] = newColumnWidth;
    isColumnResizing = false;
    this.forceUpdate(); // TODO: move to store + state
  }

  columnGetter(key, rowIndex) {
    let row = this.rowGetter(rowIndex);
    if ("user" == key) {
      return CellRenderers.user(row.user);
    } else if ("context" == key) {
      return CellRenderers.context(row.context);
    } else if ("query" == key) {
      return CellRenderers.query(row.query, row.context);
    } else if ("status" == key) {
      return CellRenderers.status("", "status", row);
    } else if ("started" == key) {
      return CellRenderers.started(row.started);
    } else if ("duration" == key) {
      return row.duration;
    } else if ("output" == key) {
      return CellRenderers.output(row.output, "output", row, this);
    }
  }

  rowGetter(rowIndex) {
    //Show latest on top
    let descIndex = this.state.runs.length - rowIndex - 1;
    return formatRun(this.state.runs[descIndex], this.state.user);
  }

  renderEmptyMessage() {
    return (
      <p className="info panel-body text-light text-center">No queries to show</p>
    );
  }

  /* Store events */
  _onChange() {
    this.setState(this.getStateFromStore());
  }
}

function formatRun(run, currentUser) {
  if (!run) return;
  return {
    user: run.user,
    context: run.sessionContext,
    query: run.query,
    status: run.state,
    infoUri: run.infoUri,
    started: run.queryStarted,
    duration: run.queryStats && run.queryStats.elapsedTime,
    output: run.output && run.output,
    _run: run,
    _currentUser: currentUser
  };
}

function selectQuery(query, sessionContext, e) {
  e.preventDefault();
  QueryActions.selectQuery({query, sessionContext});
}

function selectTable(table, e) {
  e.preventDefault();
  TableActions.addTable({
    name: table
  });
  TableActions.selectTable(table);
  TabActions.selectTab(TabConstants.DATA_PREVIEW);
}

function previewQueryResult(file, query, e) {
  let pageNum = 1;
  let pageSize = 10;
  e.preventDefault();
  ResultsPreviewActions.selectPreviewQuery(query);
  ResultsPreviewActions.loadResultsPreview(file, pageNum, pageSize);
  TabActions.selectTab(TabConstants.RESULTS_PREVIEW);
}

function killRun(uuid) {
  RunActions.kill(uuid);
}

function toggleModal(component, msg=null) {
  let state = component.state;
  if (!state.showModal) {
    state.currentErrorMessage = msg;
    state.showModal = true;
  }
  else {
    state.currentErrorMessage = null;
    state.showModal = false;
  }
  component.setState(state);
}

let CellRenderers = {
  user(cellData) {
    return <span title={cellData}>{cellData}</span>;
  },

  context(sessionContext) {
    let contextTitle = "Catalog: " + sessionContext.catalog + "\nSchema: " + sessionContext.schema + "\n";
    contextTitle += "Properties: \n"
    for (const [key, value] of Object.entries(sessionContext.properties)) {
      contextTitle += "  " + key + " = " + value
    }
    return (
        <span title={contextTitle}>{sessionContext.catalog + "." + sessionContext.schema}</span>
    );
  },

  query(query, sessionContext) {
    return (
        <a href="#" onClick={selectQuery.bind(null, query, sessionContext)}>
          <code title={query}>{query}</code>
        </a>
    );
  },

  status(cellData, cellDataKey, rowData) {
    let run = rowData._run;
    let link = (run.infoUri != null && run.infoUri != "") ? run.infoUri : "#";
    if (run.state === RunStateConstants.FAILED) {
      return (<span><span className="label label-danger" style={{minWidth:"61px"}}><a href={link} target="_blank">FAILED</a></span><a href="#" title="Queries may fail or be cancelled if they exceed configured resources limits.This includes limits on memory consumption and result file size"><i className="fa fa-info" style={{top:'3px',color:'#d5534f',marginLeft:'5px'}}></i></a></span>);
    } else if (run.state === RunStateConstants.FINISHED) {
      return (<span className="label label-success" style={{minWidth:"61px"}}><a href={link} target="_blank">{run.state}</a></span>);
    } else if (run.state === RunStateConstants.QUEUED) {
      return (<span className="label label-default" style={{minWidth:"61px"}}><a href={link} target="_blank">{run.state}</a></span>);
    } else {
      return (<span className="label label-info" style={{minWidth:"61px"}}><a href={link} target="_blank">{run.state}</a></span>);
    }
  },

  output(cellData, cellDataKey, rowData, parentObj) {
    let run = rowData._run;
    let currentUser = rowData._currentUser;
    let killable = currentUser && currentUser.name === run.user;
    let output = cellData;
    if (output && output.location && (run.state !== RunStateConstants.FAILED)) {
      //Support relative URLs
      if (output.location[0] === '/' || output.location.indexOf('http') != -1
          || output.location.indexOf('..') == 0) {
        return (
          <div>
            <a href={output.location} target="_blank" className='btn'>
              Download CSV
              <i className='glyphicon glyphicon-download' />
            </a>
            <a
              href="#"
              onClick={previewQueryResult.bind(
                  null,
                  output.location,
                  {
                    query: run.query,
                    sessionContext: run.sessionContext
                  }
              )}
              className='btn'>
              Preview Results
            </a>
          </div>
        );
      } else {
        return (
          <a href="#" onClick={selectTable.bind(null, output.location)}>
            <code title={output.location}>{output.location}</code>
          </a>
        );
      }
    } else if (run.state === 'RUNNING'
        || run.state === RunStateConstants.QUEUED
        || run.state === RunStateConstants.RESCHEDULING
        || run.state === RunStateConstants.RESUMING) {
      let progressFromStats = getProgressFromStats(run.queryStats);
      return (
        <div className={classNames({
          'runs-table-progress': true,
          'runs-table-progress-killable': killable
        })}>
          <ProgressBar progress={progressFromStats} />
          {killable ?
            <span className="glyphicon glyphicon-remove text-danger"
              title="Kill query"
              onClick={killRun.bind(null, run.uuid)}></span>
          : null}
        </div>
        );

    // XXX this needs to be a modal...we can use a custom modal here or something experimental
    } else if (run.state === RunStateConstants.FAILED) {
      let message = (run.error != undefined) ? run.error.message : "Unknown";
      return (
          <div>
            <button className={"btn btn-error"}
                    onClick={toggleModal.bind(null, parentObj, message)}>Query ERROR</button>
          </div>
      );
    }
  },

  started(cellData) {
    // let m = moment.utc(cellData, 'yyyy-mm-ddTHH:MM:ss');
    // let utc = m.format();
    // let human = m.format('lll');
    // return <span title={utc}>{human} UTC</span>;
    return <Moment utc local>{cellData}</Moment>
  }
};

function getProgressFromStats(stats) {
  if (!stats || !stats.progressPercentage) {
    return 0.0;
  } else {
    return stats.progressPercentage;
  }
}

export default RunsTable;
