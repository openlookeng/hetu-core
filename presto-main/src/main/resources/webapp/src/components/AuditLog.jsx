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

import React from "react";
import Header from "../queryeditor/components/Header";
import Footer from "../queryeditor/components/Footer";
import StatusFooter from "../queryeditor/components/StatusFooter";
import NavigationMenu from "../NavigationMenu";
import _ from "lodash";
import TabStore from "../queryeditor/stores/TabStore";
import RunActions from "../queryeditor/actions/RunActions";
import TableStore from "../queryeditor/stores/TableStore";
import AuditLogActions from "../queryeditor/actions/AuditLogActions";
import MultiSelect from 'react-simple-multi-select';

export class AuditLog extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            auditlog:{},
            searchStringUser: '',
            searchStringDate:'',
            searchStringBeginTime:'',
            searchStringEndTime:'',
            searchStringLogLevel:'INFO',
            searchStringType:'Sql',
            selectedTab: TabStore.getSelectedTab(),
            currentPage: 1,
            pageSize: 10,
            total: 0,
            tableWidth: 400,
            tableHeight: 400,
            checkStatusLogLevel:{
                INFO:true,
                WARN:true
            },
            checkStatusType: {
                Sql: true,
                WebUI: true,
                Cluster: true
            },
            itemListLogLevel: [
                {key: "INFO", value: "INFO"},
                {key: "WARN", value: "WARN"}
            ],
            selectedItemListLogLevel: [
                {key: "INFO", value: "INFO"},
                {key: "WARN", value: "WARN"}
            ],
            itemListType: [
                {key: "Sql", value: "Sql"},
                {key: "WebUI", value: "WebUi"},
                {key: "Cluster", value: "Cluster"}
            ],
            selectedItemListType: [
                {key: "Sql", value: "Sql"},
                {key: "WebUI", value: "WebUi"},
                {key: "Cluster", value: "Cluster"}
            ],
        };
        this.flag = {
            searchStringUser: '',
            searchStringBeginTime:'',
            searchStringEndTime:'',
            searchStringLogLevel:'',
            searchStringType:''
        }

        this.refreshLoop = this.refreshLoop.bind(this);
        this.handleSearchStringChange = this.handleSearchStringChange.bind(this);
        this.renderDateRangePicker = this.renderDateRangePicker.bind(this);
        this.handleSortClick = this.handleSortClick.bind(this);
        this.refreshData = this.refreshData.bind(this);
        this.onPageChange = this.onPageChange.bind(this);
        this.onPageSizeChange = this.onPageSizeChange.bind(this);
        this.debounceSearch = _.debounce(() => { _.defer(this.refreshData) }, 1000)
        this.update = this.update.bind(this);
        this.onChange = this.onChange.bind(this);
        this.onResize = this.onResize.bind(this);
        this.onTabChange = this.onTabChange.bind(this);
        this.renderAuditLog = this.renderAuditLog.bind(this);
        this.handleDownload = this.handleDownload.bind(this);
        // this.onChangeListLogLevel = this.onChangeListLogLevel.bind(this);
        // this.onChangeListType = this.onChangeListType.bind(this);
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
        const {searchStringUser,searchStringBeginTime,searchStringEndTime,searchStringLogLevel,searchStringType} = this.state;
        if (this.flag.searchStringUser != searchStringUser || this.flag.searchStringBeginTime != searchStringBeginTime || this.flag.searchStringEndTime != searchStringEndTime||
            this.flag.searchStringLogLevel != searchStringLogLevel || this.flag.searchStringType != searchStringType) {
            AuditLogActions.getAuditLog(searchStringUser, searchStringBeginTime,
                searchStringEndTime, searchStringLogLevel, searchStringType).then((data) => {
                this.setState({
                    auditlog: data
                })
                this.resetTimer();
            })
        }

        this.flag = {searchStringUser,searchStringBeginTime,searchStringEndTime,searchStringLogLevel,searchStringType};

    }

    componentDidMount() {
        this.refreshLoop();
        RunActions.connect();
        TableStore.listen(this.onChange);
        TabStore.listen(this.onTabChange);

        this.update();
        let win = window;

        if (win.addEventListener) {
            win.addEventListener('resize', this.onResize, false);
        } else if (win.attachEvent) {
            win.attachEvent('onresize', this.onResize);
        } else {
            win.onresize = this.onResize;
        }

        $(window).on('resize', this.update);
    }

    componentWillUnmount() {
        RunActions.disconnect();
        TableStore.unlisten(this.onChange);
        TabStore.unlisten(this.onTabChange);
    }

    update() {
        let windowHeight = document.body.clientHeight;
        let windowWidth = document.documentElement.clientWidth;
        let newWidth = windowWidth - 75 - 270 - 10; //left side size
        newWidth = 400 > newWidth ? 400 : newWidth;
        let newHeight = windowHeight - ((0.3 * (windowHeight + 50)) + 40 + 57 + 46 + 6); // editor + header + tabs + footer+ extra 6(unknown) heights
        newHeight = 400 > newHeight ? 400 : newHeight;
        this.setState({
            tableWidth: newWidth,
            tableHeight: newHeight,
        });
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
        else if (event.target.name === "LogLevel") {
            let obj = document.getElementById('loglevel');
            let index = obj.selectedIndex;
            this.setState({
                currentPage: 1,
                searchStringLogLevel: obj.options[index].value
            })
        }
        else if (event.target.name === "AuditType") {
            let obj = document.getElementById('audittype');
            let index = obj.selectedIndex;
            this.setState({
                currentPage: 1,
                searchStringType: obj.options[index].value
            })
        }

        this.debounceSearch();
    }

    // onChangeListLogLevel(selectedItemList) {
    //     this.state.itemListLogLevel.map(item => {this.state.checkStatusLogLevel[item.value]=false})
    //     selectedItemList.map(item => {this.state.checkStatusLogLevel[item.value]=true})
    //     let state = this.state;
    //     state.selectedItemListLogLevel = selectedItemList;
    //     let LogLevelString = "";
    //     if (this.state.checkStatusLogLevel.INFO && this.state.checkStatusLogLevel.WARN) {
    //         LogLevelString = "INFO,WARN";
    //     }
    //     else if (this.state.checkStatusLogLevel.WARN) {
    //         LogLevelString = "WARN";
    //     }
    //     else if (this.state.checkStatusLogLevel.INFO) {
    //         LogLevelString = "INFO";
    //     }
    //     this.state.searchStringLogLevel = LogLevelString;
    //     this.setState(state);
    // }

    // onChangeListType(selectedItemList) {
    //     this.state.itemListType.map(item => {this.state.checkStatusType[item.value]=false})
    //     selectedItemList.map(item => {this.state.checkStatusType[item.value]=true})
    //     let state = this.state;
    //     state.selectedItemListType = selectedItemList;
    //     this.setState(state);
    // }

    renderDateRangePicker() {
        let that = this;
        $('#date-picker').daterangepicker({
            "timePicker": true,
            "timePicker24Hour": true,
            "linkedCalendars": false,
            "autoUpdateInput": false,
            "locale": {
                format: 'YYYY-MM-DD.HH:mm',
                separator: ' ~ ',
                applyLabel: "确定",
                cancelLabel: "取消",
                resetLabel: "重置",
            }
        }, function(start, end, label) {
            if(!this.startDate){
                this.element.val('');
            }else{
                this.element.val(this.startDate.format(this.locale.format) + this.locale.separator + this.endDate.format(this.locale.format));
                that.state.searchStringDate = this.startDate.format(this.locale.format) + this.locale.separator + this.endDate.format(this.locale.format);
                that.state.searchStringBeginTime = this.startDate.format(this.locale.format);
                that.state.searchStringEndTime = this.endDate.format(this.locale.format);
                setTimeout(that.debounceSearch,0);
            }
        });

    }

    handleDownload() {
        const {searchStringUser,searchStringBeginTime,searchStringEndTime,searchStringLogLevel,searchStringType} = this.state;
        if (searchStringUser != '' && searchStringBeginTime != '' && searchStringEndTime != '') {
            AuditLogActions.downloadAuditLog(searchStringUser, searchStringBeginTime,
                searchStringEndTime, searchStringLogLevel, searchStringType);
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

    renderAuditLog(obj) {
        let arr = [];
        for (let i = 0; i < obj.length; i++) {
            arr.push(
                <div className="auditlog-row">
                    {obj[i]}
                </div>
            )
        }
        return arr ;
    }

    onChange() {
        const table = TableStore.getActiveTable();
        if (!table) return;
        if (this.state.dataPreview && table.name === this.state.dataPreview.name) return;

        this.setState({
            dataPreview: table,
        });

        // TabActions.selectTab.defer(TabConstants.DATA_PREVIEW);
    }

    onTabChange() {
        const selectedTab = TabStore.getSelectedTab();

        this.setState({selectedTab});
    }

    onResize() {
        this.update();
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
        const { auditlog } = this.state;
        const {searchStringUser,searchStringBeginTime,searchStringEndTime,searchStringLogLevel,searchStringType} = this.state;
        let downloadParam = {
            user: searchStringUser,
            beginTime: searchStringBeginTime,
            endTime: searchStringEndTime,
            level: searchStringLogLevel,
            type: searchStringType,
        }
        let downloadArray = [];
        _.each(downloadParam, (value, key) => {
            downloadArray.push(`${key}=${value}`);
        })
        let downloadString = downloadArray.join("&&");


        return (
            <div>
                <div className='flex flex-row flex-initial header'>
                    <Header />
                </div>
                <div className='flex flex-row content'>
                    <NavigationMenu active={"auditlog"} />
                    <div className="container">
                        <div className="log toolbar-log">
                            <div className="col-xs-12 toolbar-audit">
                                <div className="input-group-audit">
                                    <input type="text" className="form-control form-control-auditlog search-bar" placeholder="User" name="User"
                                           onChange={this.handleSearchStringChange} value={this.state.searchStringUser} />
                                    <input type="text" id="date-picker" className="form-control form-control-auditlog search-bar" placeholder="Date" name="Date"
                                           onFocus={this.renderDateRangePicker}  onChange={this.handleSearchStringChange} value={this.state.searchStringDate}/>
                                    {/*<MultiSelect*/}
                                    {/*    title={"Log Level"}*/}
                                    {/*    itemList={this.state.itemListLogLevel}*/}
                                    {/*    selectedItemList={this.state.selectedItemListLogLevel}*/}
                                    {/*    changeList={this.onChangeListLogLevel}*/}
                                    {/*    isObjectArray={true}*/}
                                    {/*/>*/}
                                    {/*<MultiSelect*/}
                                    {/*    title={"Type"}*/}
                                    {/*    itemList={this.state.itemListType}*/}
                                    {/*    selectedItemList={this.state.selectedItemListType}*/}
                                    {/*    changeList={this.onChangeListType}*/}
                                    {/*    isObjectArray={true}*/}
                                    {/*form-control form-control-auditlog select*/}
                                    {/*/>*/}

                                    <select id="loglevel" className="selectpicker" data-style="select" onChange={this.handleSearchStringChange} value={this.state.searchStringLogLevel} name="LogLevel" >
                                        <option>INFO </option>
                                        <option>WARN </option>
                                    </select>
                                    <select id="audittype" className="selectpicker" data-style="select" onChange={this.handleSearchStringChange} value={this.state.searchStringType} name="AuditType" >
                                        <option value="Sql">Sql</option>
                                        <option value="Webui">WebUI</option>
                                        <option value="Cluster">Cluster</option>
                                    </select>
                                    <a href={"../v1/audit/download?"+downloadString} target="_blank" className="btn"> Download <i className="icon fa fa-download"/> </a>
                                </div>
                            </div>
                            <div className="col-xs-12 auditlog-text">
                                {JSON.stringify(this.state.auditlog) == "{}"  ?
                                    <div className="row error-message">
                                        <div className="col-xs-12"><h4>{"No Audit Log"}</h4></div>
                                    </div>
                                    :
                                    this.renderAuditLog(auditlog)
                                }
                            </div>
                        </div>

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

