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
    getHumanReadableState,
    getQueryStateColor,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
    truncateString
} from "../newUtils";
import Header from "../queryeditor/components/Header";
import Footer from "../queryeditor/components/Footer";
import StatusFooter from "../queryeditor/components/StatusFooter";
import NavigationMenu from "../NavigationMenu";
import Pagination from 'rc-pagination';
import Select from "rc-select";
import localeInfo from "../node_modules/rc-pagination/es/locale/en_US";
import _ from "lodash";
import ModalDialog from "../queryeditor/components/ModalDialog";

let isShowMore = true;

export class QueryListItem extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            show: false,
        }
        this.showModal = this.showModal.bind(this);
    }

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
        if (formattedQueryText && formattedQueryText.length > 550) {
            isShowMore = true;
        }
        else {
            isShowMore = false;
        }

        return truncateString(formattedQueryText, 550);
    }

    showModal(e) {
        let newShow = this.state.show;
        this.setState({
            show: !newShow
        })
    }

    getAllQueryText(queryText) {
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

        return formattedQueryText;
    }

    render() {
        const query = this.props.query;
        const progressBarStyle = {width: 100 + "%", backgroundColor: getQueryStateColor(query) };

        const splitDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Completed splits" data-container="body">
                    <span className="glyphicon glyphicon-ok" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.completedDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Running splits" data-container="body">
                    <span className="glyphicon glyphicon-play" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.runningDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Queued splits" data-container="body">
                    <span className="glyphicon glyphicon-pause" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queuedDrivers}
                </span>
            </div>);

        const timingDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Wall time spent executing the query (not including queued time)" data-container="body">
                    <span className="glyphicon glyphicon-hourglass" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.executionTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Total query wall time" data-container="body">
                    <span className="glyphicon glyphicon-time" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.elapsedTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="CPU time spent by this query" data-container="body">
                    <span className="glyphicon glyphicon-dashboard" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.totalCpuTime}
                </span>
            </div>);

        const memoryDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Current total reserved memory" data-container="body">
                    <span className="glyphicon glyphicon-scale" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.totalMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Peak total memory" data-container="body">
                    <span className="glyphicon glyphicon-fire" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {query.peakTotalMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Cumulative user memory" data-container="body">
                    <span className="glyphicon glyphicon-equalizer" style={GLYPHICON_HIGHLIGHT} />&nbsp;&nbsp;
                    {formatDataSizeBytes(query.cumulativeUserMemory / 1000.0)}
                </span>
            </div>);

        let user = (<span>{query.user}</span>);
        // if (query.session.principal) {
        //     user = (
        //         <span>{query.user}<span className="glyphicon glyphicon-lock-inverse" style={GLYPHICON_DEFAULT} /></span>
        //     );
        // }

        return (
            <div className="query">
                <div className="row">
                    <div className="col-xs-4">
                        <div className="row stat-row query-header query-header-queryid">
                            <div className="col-xs-9" data-toggle="tooltip" data-placement="bottom" title="Query ID">
                                <a href={"/ui/query.html?history_"+query.queryId} target="_blank">{query.queryId}</a>
                            </div>
                            <div className="col-xs-3 query-header-timestamp" data-toggle="tooltip" data-placement="bottom" title="Submit time">
                                <span>{formatShortTime(new Date(Date.parse(query.createTime)))}</span>
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
                                    <span>{truncateString(query.source, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Resource Group">
                                    <span className="glyphicon glyphicon-road" style={GLYPHICON_DEFAULT} />&nbsp;&nbsp;
                                    <span>{truncateString(query.resource ? query.resource : "n/a", 35)}</span>
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
                                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuemin="0"
                                         aria-valuemax="100" style={progressBarStyle}>
                                        <div className="progress-catalog-schema-container">
                                            <span className="progress-catalog-schema">
                                                catalog:
                                            </span>
                                            <span className="text-catalog-schema">
                                                {query.catalog}
                                            </span>
                                            &nbsp;&nbsp;
                                            <span className="progress-catalog-schema">
                                                schema:
                                            </span>
                                            <span className="text-catalog-schema">
                                                {query.schemata}
                                            </span>
                                        </div>
                                        <div className="progress-state">
                                            {getHumanReadableState(query)}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="row query-row-bottom">
                            <div className="col-xs-12">
                                <pre className="query-snippet"><code className="sql">{QueryListItem.stripQueryTextWhitespace(query.query)}</code></pre>
                                {isShowMore ?
                                    <div className="query-showmore">
                                        <a onClick={this.showModal} style={{color: '#1E90FF',cursor: 'pointer'}}>
                                            <span className="glyphicon glyphicon-arrow-right" style={{color: '#1E90FF'}}/>
                                            &nbsp;Show More
                                        </a>
                                        <ModalDialog onClose={this.showModal} header={getHumanReadableState(query)} footer={""}
                                                     show={this.state.show}>
                                            <div className="modal-querytext">
                                                {this.getAllQueryText(query.query)}
                                            </div>
                                        </ModalDialog>
                                    </div>
                                    :
                                    null
                                }
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
            searchStringUser: '',
            searchStringDate:'',
            searchStringSql:'',
            searchStringQueryId:'',
            searchStringResourceGroup:'',
            currentPage: 1,
            pageSize: 10,
            total: 0,
            queryString: '',
            queryNums: -1,
            metaStoreType:''
        };

        this.refreshLoop = this.refreshLoop.bind(this);
        this.handleSearchStringChange = this.handleSearchStringChange.bind(this);
        this.renderDateRangePicker = this.renderDateRangePicker.bind(this);
        this.handleSortClick = this.handleSortClick.bind(this);
        this.refreshData = this.refreshData.bind(this);
        this.onPageChange = this.onPageChange.bind(this);
        this.onPageSizeChange = this.onPageSizeChange.bind(this);
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
        const { stateFilters, errorTypeFilters, currentSortType, currentSortOrder, searchStringUser,searchStringDate,searchStringSql
            ,searchStringQueryId,searchStringResourceGroup,currentPage, pageSize } = this.state;
        let queryParam = {
            state: stateFilters.join(","),
            failed: errorTypeFilters.join(","),
            sort: currentSortType,
            sortOrder: currentSortOrder,
            searchUser: searchStringUser,
            searchDate:searchStringDate,
            searchSql:searchStringSql,
            searchQueryId:searchStringQueryId,
            searchResourceGroup:searchStringResourceGroup,
            pageNum: currentPage,
            pageSize: pageSize,
        }
        let queryArray = [];
        _.each(queryParam, (value, key) => {
            queryArray.push(`${key}=${value}`);
        })
        let queryString = queryArray.join("&&");
        $.get(`../v1/query/queryNums`, function (num) {
            let queryNum = this.state.queryNums;
            if(queryNum==num){

            }
            else {
                this.state.queryNums = num;
                $.get(`../v1/query/queryInfo?${queryString}`, function (queryList) {
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
            this.resetTimer();
        }.bind(this))
        if (queryString !== this.state.queryString) {
            $.get(`../v1/query/queryInfo?${queryString}`, function (queryList) {
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

        this.setState({
            queryString : queryString
        })

    }

    componentDidMount() {
        $.get(`../v1/query/hetuMetastoreType`, function (metaStoreType) {
            console.log(metaStoreType);
            this.state.metaStoreType = metaStoreType;
            if(this.state.metaStoreType != "jdbc") {
                alert("Collection failed! Only hetu-metastore storage type configured as JDBC is supported!");
            }
            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.resetTimer();
            }.bind(this));

        this.refreshLoop();
    }

    handleSearchStringChange(event) {
        const newSearchString = event.target.value;
        if (event.target.name === "User") {
            this.setState({
                currentPage: 1,
                searchStringUser: newSearchString
            });
        }
        else if (event.target.name === "Date"){
            this.setState({
                currentPage: 1,
                searchStringDate: newSearchString
            });
        }
        else if (event.target.name === "Sql"){
            this.setState({
                currentPage: 1,
                searchStringSql: newSearchString
            });
        }
        else if (event.target.name === "QueryId"){
            this.setState({
                currentPage: 1,
                searchStringQueryId: newSearchString
            });
        }
        else if (event.target.name === "ResourceGroup"){
            this.setState({
                currentPage: 1,
                searchStringResourceGroup: newSearchString
            })
        }

        this.debounceSearch();
    }

    renderDateRangePicker() {
        let that = this;
        $('#date-picker').daterangepicker({
            "timePicker": true,
            "timePicker24Hour": true,
            "linkedCalendars": false,
            "autoUpdateInput": false,
            "timePickerMinutes": true,
            "locale": {
                format: 'YYYY-MM-DD.HH:mm',
                separator: ' ~ ',
                applyLabel: "apply",
                cancelLabel: "clear",
                resetLabel: "reset",
            }
        }, function(start, end, label) {

        });

        $('#date-picker').on('apply.daterangepicker', function(ev, picker) {
            picker.element.val(picker.startDate.format(picker.locale.format) + picker.locale.separator + picker.endDate.format(picker.locale.format));
            that.state.searchStringDate = picker.startDate.format(picker.locale.format) + picker.locale.separator + picker.endDate.format(picker.locale.format);
            setTimeout(that.debounceSearch,0);
        });

        $('#date-picker').on('hide.daterangepicker', function(ev, picker) {
            if(that.state.searchStringDate !== "") {
                that.state.searchStringDate = picker.startDate.format(picker.locale.format) + picker.locale.separator + picker.endDate.format(picker.locale.format);
            }
            setTimeout(that.debounceSearch,0);
        });

        $('#date-picker').on('cancel.daterangepicker', function(ev, picker) {
            $('#date-picker').val('');
            that.state.searchStringDate = "";
            setTimeout(that.debounceSearch,0);
        });
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
        document.getElementById("query-history-sort").setAttribute("class", "input-group-btn open")
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
            stateSelectAll = newFilters.length == Object.keys(FILTER_TYPE).length ? true : false;
        }
        this.setState({
            currentPage: 1,
            stateSelectAll,
            stateFilters: newFilters,
        });
        _.defer(this.refreshData);
        document.getElementById("query-history-filter").setAttribute("class", "input-group-btn open")
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
            errorSelectAll = newFilters.length == Object.keys(ERROR_TYPE).length ? true : false;
        }

        this.setState({
            currentPage: 1,
            errorSelectAll,
            errorTypeFilters: newFilters,
        });
        _.defer(this.refreshData);
        document.getElementById("query-history-error").setAttribute("class", "input-group-btn open")
    }

    onPageChange(current, pageSize) {
        this.setState({ currentPage: current });
        _.defer(this.refreshData);
    }

    onPageSizeChange(current, pageSize) {
        this.setState({ pageSize: pageSize });
        _.defer(this.refreshData);
    }

    render() {
        const { allQueries, currentPage, total, stateFilters, pageSize } = this.state;
        // let queryList = <DisplayedQueriesList queries={allQueries} />;
        let queryList;
        if (allQueries == null || total === 0) {
            let label = (<div className="loader">Loading...</div>);
            if (allQueries == null || total === 0) {
                label = "No queries";
            }
            queryList = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }
        else {
            queryList = <DisplayedQueriesList queries={allQueries} />;
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
                            <div className="col-xs-12 toolbar-col query-history">
                                <div className="input-group input-group-sm">
                                    <input type="text" className="form-control form-control-small search-bar" placeholder="User" name="User"
                                           onChange={this.handleSearchStringChange} value={this.state.searchStringUser} />
                                    <input type="text" id="date-picker" className="form-control form-control-small search-bar" placeholder="Date" name="Date"
                                           onFocus={this.renderDateRangePicker}  onChange={this.handleSearchStringChange}
                                           value={this.state.searchStringDate}/>
                                    <input type="text" className="form-control form-control-small search-bar" placeholder="Sql" name="Sql"
                                           onChange={this.handleSearchStringChange} value={this.state.searchStringSql} />
                                    <input type="text" className="form-control form-control-small search-bar" placeholder="QueryId" name="QueryId"
                                           onChange={this.handleSearchStringChange} value={this.state.searchStringQueryId} />
                                    <input type="text" className="form-control form-control-small search-bar" placeholder="ResourceGroup" name="ResourceGroup"
                                            onChange={this.handleSearchStringChange} value={this.state.searchStringResourceGroup}/>
                                    <div className="input-group-btn" id="query-history-filter">
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
                                        <div className="input-group-btn" id="query-history-error">
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
                                    <div className="input-group-btn" id="query-history-sort">
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
                        <Pagination
                            defaultCurrent={1}
                            current={currentPage}
                            total={total}
                            defaultPageSize={10}
                            pageSize={pageSize}
                            onChange={this.onPageChange}
                            showTotal={total => `Total ${total} queries`}
                            style={{ marginTop: 10 }}
                            locale={localeInfo}
                            showSizeChanger
                            onShowSizeChange={this.onPageSizeChange}
                            selectComponentClass={Select}
                        />
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

