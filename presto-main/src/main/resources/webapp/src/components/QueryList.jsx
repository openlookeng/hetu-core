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

import React, { Fragment } from "react";

import {
    formatDataSizeBytes,
    formatShortTime,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
    truncateString
} from "../utils";
import Header from "../queryeditor/components/Header";
import Footer from "../queryeditor/components/Footer";
import StatusFooter from "../queryeditor/components/StatusFooter";
import NavigationMenu from "../NavigationMenu";
import Pagination from 'rc-pagination';

export class QueryListItem extends React.Component {
    static stripQueryTextWhitespace(queryText) {
        const lines = queryText.split("\n");
        let minLeadingWhitespace = -1;
        for (let i = 0; i < lines.length; i++) {
            if (minLeadingWhitespace === 0) {
                break;
            }

            if (lines[i].trim().length === 0) {
                continue;
            }

            const leadingWhitespace = lines[i].search(/\S/);

            if (leadingWhitespace > -1 && ((leadingWhitespace < minLeadingWhitespace) || minLeadingWhitespace === -1)) {
                minLeadingWhitespace = leadingWhitespace;
            }
        }

        let formattedQueryText = "";

        for (let i = 0; i < lines.length; i++) {
            const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, '');

            if (trimmedLine.length > 0) {
                formattedQueryText += trimmedLine;

                if (i < (lines.length - 1)) {
                    formattedQueryText += "\n";
                }
            }
        }

        return truncateString(formattedQueryText, 300);
    }

    render() {
        const query = this.props.query;
        const progressBarStyle = { width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query) };

        const splitDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Completed splits">
                    <span className="glyphicon glyphicon-ok" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.completedDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Running splits">
                    <span className="glyphicon glyphicon-play" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.runningDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Queued splits">
                    <span className="glyphicon glyphicon-pause" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.queuedDrivers}
                </span>
            </div>);

        const timingDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Wall time spent executing the query (not including queued time)">
                    <span className="glyphicon glyphicon-hourglass" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.executionTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Total query wall time">
                    <span className="glyphicon glyphicon-time" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.elapsedTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="CPU time spent by this query">
                    <span className="glyphicon glyphicon-dashboard" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.totalCpuTime}
                </span>
            </div>);

        const memoryDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Current total reserved memory">
                    <span className="glyphicon glyphicon-scale" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.totalMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Peak total memory">
                    <span className="glyphicon glyphicon-fire" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.queryStats.peakTotalMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Cumulative user memory">
                    <span className="glyphicon glyphicon-equalizer" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {formatDataSizeBytes(query.queryStats.cumulativeUserMemory / 1000.0)}
                </span>
            </div>);

        let user = (<span>{query.session.user}</span>);
        if (query.session.principal) {
            user = (
                <span>{query.session.user}<span className="glyphicon glyphicon-lock-inverse" style={GLYPHICON_DEFAULT} /></span>
            );
        }

        return (
            <div className="query">
                <div className="row">
                    <div className="col-xs-4">
                        <div className="row stat-row query-header query-header-queryid">
                            <div className="col-xs-9" data-toggle="tooltip" data-placement="bottom" title="Query ID">
                                <a href={"" + query.self.replace(/.*\/v1\/query\//, "query.html?")} target="_blank">{query.queryId}</a>
                            </div>
                            <div className="col-xs-3 query-header-timestamp" data-toggle="tooltip" data-placement="bottom" title="Submit time">
                                <span>{formatShortTime(new Date(Date.parse(query.queryStats.createTime)))}</span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="User">
                                    <span className="glyphicon glyphicon-user" style={GLYPHICON_DEFAULT} />&nbsp;&nbsp;
                                    <span>{truncateString(user, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Source">
                                    <span className="glyphicon glyphicon-log-in" style={GLYPHICON_DEFAULT} />&nbsp;&nbsp;
                                    <span>{truncateString(query.session.source, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Resource Group">
                                    <span className="glyphicon glyphicon-road" style={GLYPHICON_DEFAULT} />&nbsp;&nbsp;
                                    <span>{truncateString(query.resourceGroupId ? query.resourceGroupId.join(".") : "n/a", 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            {splitDetails}
                        </div>
                        <div className="row stat-row">
                            {timingDetails}
                        </div>
                        <div className="row stat-row">
                            {memoryDetails}
                        </div>
                    </div>
                    <div className="col-xs-8">
                        <div className="row query-header">
                            <div className="col-xs-12 query-progress-container">
                                <div className="progress">
                                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={getProgressBarPercentage(query)} aria-valuemin="0"
                                        aria-valuemax="100" style={progressBarStyle}>
                                        {getProgressBarTitle(query)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="row query-row-bottom">
                            <div className="col-xs-12">
                                <pre className="query-snippet"><code className="sql">{QueryListItem.stripQueryTextWhitespace(query.query)}</code></pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

class DisplayedQueriesList extends React.Component {
    render() {
        const queryNodes = this.props.queries.map(function (query) {
            return (
                <QueryListItem key={query.queryId} query={query} />
            );
        }.bind(this));
        return (
            <div className="queryListContainer">
                {queryNodes}
            </div>
        );
    }
}

const SELECT_ALL = "all";

const FILTER_TYPE = {
    QUEUED: "queued",
    WAITING_FOR_RESOURCES: "waiting_for_resources",
    DISPATCHING: "dispatching",
    PLANNING: "planning",
    STARTING: "starting",
    RUNNING: "running",
    FINISHING: "finishing",
    FINISHED: "finished",
    FAILED: "failed",
};

const SORT_TYPE = {
    CREATED: "creation",
    ELAPSED: "elapsed",
    EXECUTION: "execution",
    CPU: "cpu",
    CUMULATIVE_MEMORY: "cumulative",
    CURRENT_MEMORY: "memory",
};

const ERROR_TYPE = {
    USER_ERROR: "user_error",
    INTERNAL_ERROR: "internal_error",
    INSUFFICIENT_RESOURCES: "insufficient_resources",
    EXTERNAL: "external",
};

const SORT_ORDER = {
    ASCENDING: "ascending",
    DESCENDING: "descending",
};

export class QueryList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            allQueries: [],
            currentSortType: SORT_TYPE.CREATED,
            currentSortOrder: SORT_ORDER.DESCENDING,
            stateFilters: [
                FILTER_TYPE.QUEUED,
                FILTER_TYPE.WAITING_FOR_RESOURCES,
                FILTER_TYPE.DISPATCHING,
                FILTER_TYPE.PLANNING,
                FILTER_TYPE.STARTING,
                FILTER_TYPE.RUNNING,
                FILTER_TYPE.FINISHING,
                FILTER_TYPE.FINISHED,
                FILTER_TYPE.FAILED],
            errorTypeFilters: [
                ERROR_TYPE.INTERNAL_ERROR,
                ERROR_TYPE.INSUFFICIENT_RESOURCES,
                ERROR_TYPE.EXTERNAL,
                ERROR_TYPE.USER_ERROR,
            ],
            stateSelectAll: true,
            errorSelectAll: true,
            searchString: '',
            currentPage: 1,
            pageSize: 10,
            total: 0,
        };

        this.refreshLoop = this.refreshLoop.bind(this);
        this.handleSearchStringChange = this.handleSearchStringChange.bind(this);
        this.handleSortClick = this.handleSortClick.bind(this);
        this.refreshData = this.refreshData.bind(this);
        this.onPageChange = this.onPageChange.bind(this);
        this.debounceSearch = _.debounce(() => { _.defer(this.refreshData) }, 200)
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        this.refreshData();
    }

    refreshData() {
        const { stateFilters, errorTypeFilters, currentSortType, currentSortOrder, searchString,
            currentPage, pageSize } = this.state;
        let queryParam = {
            state: stateFilters.join(","),
            failed: errorTypeFilters.join(","),
            sort: currentSortType,
            sortOrder: currentSortOrder,
            search: searchString,
            pageNum: currentPage,
            pageSize: pageSize,
        }
        let queryArray = [];
        _.each(queryParam, (value, key) => {
            queryArray.push(`${key}=${value}`);
        })
        let queryString = queryArray.join("&&");

        $.get(`../v1/query?${queryString}`, function (queryList) {
            this.setState({
                allQueries: queryList.queries,
                total: queryList.total,
            });
            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.resetTimer();
            }.bind(this));

    }

    componentDidMount() {
        this.refreshLoop();
    }

    handleSearchStringChange(event) {
        const newSearchString = event.target.value;

        this.setState({
            currentPage: 1,
            searchString: newSearchString
        });

        this.debounceSearch();
    }


    renderSortListItem(sortType, sortText) {
        if (this.state.currentSortType === sortType) {
            const directionArrow = this.state.currentSortOrder === SORT_ORDER.ASCENDING ? <span className="glyphicon glyphicon-triangle-top" /> :
                <span className="glyphicon glyphicon-triangle-bottom" />;
            return (
                <li>
                    <a href="#" className="selected" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText} {directionArrow}
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText}
                    </a>
                </li>);
        }
    }

    handleSortClick(sortType) {
        const newSortType = sortType;
        let newSortOrder = SORT_ORDER.DESCENDING;

        if (this.state.currentSortType === sortType && this.state.currentSortOrder === SORT_ORDER.DESCENDING) {
            newSortOrder = SORT_ORDER.ASCENDING;
        }
        this.setState({
            currentPage: 1,
            currentSortType: newSortType,
            currentSortOrder: newSortOrder
        });
        _.defer(this.refreshData);
    }

    renderFilterButton(filterType, filterText) {
        const { stateSelectAll } = this.state;
        let checkmarkStyle = { color: '#ffffff' };
        if (this.state.stateFilters.indexOf(filterType) > -1 ||
            (filterType == SELECT_ALL && stateSelectAll)) {
            checkmarkStyle = GLYPHICON_HIGHLIGHT;
        }
        return (
            <li>
                <a href="#" onClick={this.handleStateFilterClick.bind(this, filterType)}>
                    <span className="glyphicon glyphicon-ok" style={checkmarkStyle} />
                    &nbsp;{filterText}
                </a>
            </li>);
    }

    handleStateFilterClick(filter) {
        let newFilters = [];
        let { stateSelectAll } = this.state;
        if (filter == SELECT_ALL) {
            !stateSelectAll && _.each(FILTER_TYPE, (value, key) => {
                newFilters.push(value);
            })
            stateSelectAll = !stateSelectAll;
        } else {
            newFilters = this.state.stateFilters.slice();
            if (this.state.stateFilters.indexOf(filter) > -1) {
                newFilters.splice(newFilters.indexOf(filter), 1);
            }
            else {
                newFilters.push(filter);
            }
        }

        this.setState({
            currentPage: 1,
            stateSelectAll,
            stateFilters: newFilters,
        });
        _.defer(this.refreshData);
    }

    renderErrorTypeListItem(errorType, errorTypeText) {
        const { errorSelectAll } = this.state;
        let checkmarkStyle = { color: '#ffffff' };
        if (this.state.errorTypeFilters.indexOf(errorType) > -1 ||
            (errorType == SELECT_ALL && errorSelectAll)) {
            checkmarkStyle = GLYPHICON_HIGHLIGHT;
        }
        return (
            <li>
                <a href="#" onClick={this.handleErrorTypeFilterClick.bind(this, errorType)}>
                    <span className="glyphicon glyphicon-ok" style={checkmarkStyle} />
                    &nbsp;{errorTypeText}
                </a>
            </li>);
    }

    handleErrorTypeFilterClick(errorType) {
        let newFilters = [];
        let { errorSelectAll } = this.state;
        if (errorType == SELECT_ALL) {
            !errorSelectAll && _.each(ERROR_TYPE, (value, key) => {
                newFilters.push(value);
            })
            errorSelectAll = !errorSelectAll;
        } else {
            newFilters = this.state.errorTypeFilters.slice();
            if (this.state.errorTypeFilters.indexOf(errorType) > -1) {
                newFilters.splice(newFilters.indexOf(errorType), 1);
            }
            else {
                newFilters.push(errorType);
            }
        }

        this.setState({
            currentPage: 1,
            errorSelectAll,
            errorTypeFilters: newFilters,
        });
        _.defer(this.refreshData);
    }

    onPageChange(current, pageSize) {
        this.setState({ currentPage: current });
        _.defer(this.refreshData);
    }

    render() {
        const { allQueries, currentPage, total, stateFilters } = this.state;
        let queryList = <DisplayedQueriesList queries={allQueries} />;
        if (allQueries.queries === null || total === 0) {
            let label = (<div className="loader">Loading...</div>);
            if (allQueries.queries === null || total === 0) {
                label = "No queries";
            }
            queryList = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }

        return (
            <div>
                <div className='flex flex-row flex-initial header'>
                    <Header />
                </div>
                <div className='flex flex-row content'>
                    <NavigationMenu active={"queryhistory"} />
                    <div className="container">
                        <div className="row toolbar-row">
                            <div className="col-xs-12 toolbar-col">
                                <div className="input-group input-group-sm">
                                    <input type="text" className="form-control form-control-small search-bar" placeholder="User, source, query ID, resource group, or query text"
                                        onChange={this.handleSearchStringChange} value={this.state.searchString} />
                                    <span className="input-group-addon filter-addon">State:</span>
                                    <div className="input-group-btn">
                                        <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                            State <span className="caret" />
                                        </button>
                                        <ul className="dropdown-menu dropdown-menu-left">
                                            {this.renderFilterButton(SELECT_ALL, "Select All")}
                                            {this.renderFilterButton(FILTER_TYPE.QUEUED, "Queued")}
                                            {this.renderFilterButton(FILTER_TYPE.WAITING_FOR_RESOURCES, "Waiting For Resources")}
                                            {this.renderFilterButton(FILTER_TYPE.DISPATCHING, "Dispatching")}
                                            {this.renderFilterButton(FILTER_TYPE.PLANNING, "Planning")}
                                            {this.renderFilterButton(FILTER_TYPE.STARTING, "Starting")}
                                            {this.renderFilterButton(FILTER_TYPE.RUNNING, "Running")}
                                            {this.renderFilterButton(FILTER_TYPE.FINISHING, "Finishing")}
                                            {this.renderFilterButton(FILTER_TYPE.FINISHED, "Finished")}
                                            {this.renderFilterButton(FILTER_TYPE.FAILED, "Failed")}
                                        </ul>
                                    </div>
                                    &nbsp;
                                    {stateFilters.indexOf(FILTER_TYPE.FAILED) > -1 &&
                                        <Fragment>
                                            <div className="input-group-btn">
                                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                                    Failed <span className="caret" />
                                                </button>
                                                <ul className="dropdown-menu">
                                                    {this.renderErrorTypeListItem(SELECT_ALL, "Select All")}
                                                    {this.renderErrorTypeListItem(ERROR_TYPE.INTERNAL_ERROR, "Internal Error")}
                                                    {this.renderErrorTypeListItem(ERROR_TYPE.EXTERNAL, "External Error")}
                                                    {this.renderErrorTypeListItem(ERROR_TYPE.INSUFFICIENT_RESOURCES, "Resources Error")}
                                                    {this.renderErrorTypeListItem(ERROR_TYPE.USER_ERROR, "User Error")}
                                                </ul>
                                            </div>
                                            &nbsp;
                                        </Fragment>
                                    }
                                    <div className="input-group-btn">
                                        <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                            Sort <span className="caret" />
                                        </button>
                                        <ul className="dropdown-menu dropdown-menu-right">
                                            {this.renderSortListItem(SORT_TYPE.CREATED, "Creation Time")}
                                            {this.renderSortListItem(SORT_TYPE.ELAPSED, "Elapsed Time")}
                                            {this.renderSortListItem(SORT_TYPE.CPU, "CPU Time")}
                                            {this.renderSortListItem(SORT_TYPE.EXECUTION, "Execution Time")}
                                            {this.renderSortListItem(SORT_TYPE.CURRENT_MEMORY, "Current Memory")}
                                            {this.renderSortListItem(SORT_TYPE.CUMULATIVE_MEMORY, "Cumulative User Memory")}
                                        </ul>
                                    </div>
                                </div>
                            </div>
                        </div>
                        {queryList}
                        {allQueries.length > 0 &&
                            <Pagination
                                defaultCurrent={1}
                                current={currentPage}
                                total={total}
                                onChange={this.onPageChange}
                                showTotal={total => `Total ${total} items`}
                                style={{ marginTop: 10 }}
                            />
                        }
                    </div>
                </div>
                <div className='flex flex-row flex-initial statusFooter'>
                    <StatusFooter />
                </div>
                <div className='flex flex-row flex-initial footer'>
                    <Footer />
                </div>
            </div>
        );
    }
}

