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
import QueryStore from '../stores/QueryStore';
import QueryActions from '../actions/QueryActions';
import RunActions from '../actions/RunActions';
import {Button, ButtonToolbar} from 'react-bootstrap';

function getStateFromStore() {
  return {
    queries: QueryStore.getCollection().all({
      sort: true
    })
  };
}

class MySavedQueries extends React.Component {

  constructor(props) {
    super(props);
    this.displayName = 'MySavedQueries';
    this.state = this.getInitialState();
    this.maxNumOfQueryLines = (this.props.maxQueryLines === undefined ? 20 : this.props.maxQueryLines);
    this.renderChildren = this.renderChildren.bind(this);
    this.renderEmptyMessage = this.renderEmptyMessage.bind(this);
    this._onChange = this._onChange.bind(this);
    this._fetchQueries = this._fetchQueries.bind(this);
    this._runQuery = this._runQuery.bind(this);
    this._deleteQuery = this._deleteQuery.bind(this);
  }

  getInitialState() {
    return getStateFromStore();
  }

  componentDidMount() {
    QueryStore.listen(this._onChange);
    this._fetchQueries();
  }

  componentWillUnmount() {
    QueryStore.unlisten(this._onChange);
  }

  render() {
    return (
      <div className='panel-body'>
        <div>
          {this.renderChildren()}
        </div>
      </div>
    );
  }

  renderChildren() {
    if (this.state.queries.length === 0) {
      return this.renderEmptyMessage();
    } else {
      return this.state.queries.map((query) => {
        let queryText = query.queryWithPlaceholders.query;
        let noOfLinesInQuery = (queryText.match(/\n/g) || []).length;
        let linesToShow = noOfLinesInQuery > this.maxNumOfQueryLines ? this.maxNumOfQueryLines : noOfLinesInQuery;
        linesToShow = linesToShow <= 1 ? 2 : linesToShow;
        let lineHeight = 20;
        let height = linesToShow * lineHeight;
        return (
          <div key={query.uuid} className='saved-container'>
            <div>
              <h4>{query.name}</h4>
              <p>{query.description}</p>
            </div>
            <div className='clearfix'>
              <pre onClick={this._onSelectQuery.bind(null, queryText)} style={{height: height}}>
                <code>{queryText}</code>
              </pre>
              <ButtonToolbar className="pull-right">
                {!(query.featured) && <Button
                    size={"sm"}
                    onClick={this._deleteQuery.bind(null, query.uuid)}>
                  Delete
                </Button> }
                <Button
                    className={"btn btn-success active"}
                  size="sm"
                  variant={"success"}
                  onClick={this._runQuery.bind(null, queryText)}>
                    Run
                </Button>
              </ButtonToolbar>
            </div>
          </div>
        );
      });
    }
  }

  renderEmptyMessage() {
    return (
      <div className='row'>
        <div className="col-md-12 text-center">
          No sample queries
        </div>
      </div>
    );
  }

  _onChange() {
    this.setState(getStateFromStore());
  }

  _fetchQueries() {
    QueryActions.fetchSavedQueries();
  }

  _onSelectQuery(query) {
    QueryActions.selectQuery({query});
  }

  _runQuery(queryText) {
    QueryActions.selectQuery({query:queryText});
    RunActions.execute({
      query: queryText
    });
  }

  _deleteQuery(uuid) {
    QueryActions.destroyQuery(uuid);
  }
}

function truncate(text, length) {
  let output = text || '';
  if (output.length > length) {
    output = output.slice(0, length) + '...';
  }
  return output;
}

export default MySavedQueries;
