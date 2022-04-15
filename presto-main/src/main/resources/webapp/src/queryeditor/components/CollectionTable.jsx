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
import 'moment-timezone';
import UserStore from "../stores/UserStore";
import RunStore from "../stores/RunStore";
import Pagination from "rc-pagination";
import Select from "rc-select";
import _ from "lodash";
import CollectionAction from "../actions/CollectionActions";
import {Table, Column, Cell} from 'fixed-data-table-2';
import {setSelection} from "../../../vendor/vis/dist/vis";
import QueryActions from "../actions/QueryActions";

let columnWidths = {
    user: 150,
    context:200,
    query: 800,
    delete: 100,
}


class CollectionTable extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            user: UserStore.getCurrentUser().name,
            pageSize: 10,
            total: 0,
            currentPage: 1,
            allCollection: [],
            metaStoreType: ''
        }
        this.onPageChange = this.onPageChange.bind(this);
        this.onPageSizeChange = this.onPageSizeChange.bind(this);
        this.refreshData = this.refreshData.bind(this);
        this.handleDeleteButton = this.handleDeleteButton.bind(this);
        this.renderQuery = this.renderQuery.bind(this);
    }

    refreshData(){
        const {currentPage,pageSize} = this.state;
        let queryText = "pageSize=" + pageSize  + "&&" + "pageNum=" + currentPage;
        $.get(`../v1/myCollection/collection/query?${queryText}`, function (queryList) {
            this.setState({
                allCollection: queryList.queries,
                total:queryList.total
            });
        }.bind(this))
    }

    componentDidMount() {
        $.get(`../v1/query/hetuMetastoreType`, function (metaStoreType) {
            this.state.metaStoreType = metaStoreType;
            if(this.state.metaStoreType != "jdbc") {
                alert("Collection failed! Only hetu-metastore storage type configured as JDBC is supported!");
            }
        }.bind(this))

        this.refreshData();
    }

    componentWillUnmount() {
    }

    handleEvent(){
        _.defer(this.refreshData);
    }

    onPageChange(current, pageSize) {
        this.state.currentPage = current
        _.defer(this.refreshData);
    }

    onPageSizeChange(current, pageSize) {
        this.state.pageSize = pageSize;
        _.defer(this.refreshData);
    }

    handleDeleteButton(e){
        let rowIndex = e.target.id;
        let queryString = this.state.allCollection;
        let deleteText = queryString[rowIndex].query;
        let catalog = queryString[rowIndex].catalog;
        let schema = queryString[rowIndex].schemata;
        CollectionAction.deleteCollection(deleteText,catalog,schema).then();
        _.defer(this.refreshData);
    }

    renderQuery(rowIndex) {
        let sessionContext = {catalog : this.state.allCollection[rowIndex].catalog ,
                            schema : this.state.allCollection[rowIndex].schemata}
        let query = this.state.allCollection[rowIndex].query;
        return (
            <a href="#" onClick={selectQuery.bind(null, query, sessionContext)}>
                <code title={query}>{query}</code>
            </a>
        );
    }

    render() {
        let rowsCount = 0;
        let compare = this.state.total - (this.state.currentPage-1)*this.state.pageSize;
        if (compare < this.state.pageSize){
            rowsCount = compare;
        }
        else {
            rowsCount = this.state.pageSize;
        }
        const {allCollection, total, pageSize, currentPage, user} = this.state;
        if (allCollection == null){
            return (
                <p className="info panel-body text-light text-center">No queries to show</p>
            );
        }
        else {
            return (
                <div className="flex collection-table hetu-table">
                    <div className="col-xs-12 row toolbar-row">
                        <Table
                            className="collection_table"
                            rowHeight={50}
                            rowsCount={rowsCount}
                            width={this.props.tableWidth}
                            height={this.props.tableHeight}
                            headerHeight={30}>
                            <Column
                                header={<Cell>User</Cell>}
                                cell={({rowIndex}) => <Cell> {user} </Cell>}
                                width={columnWidths.user}
                            />
                            <Column
                                header={<Cell>Context</Cell>}
                                cell={({rowIndex}) =>
                                    <Cell> {allCollection[rowIndex].catalog}.{allCollection[rowIndex].schemata}</Cell>}
                                width={columnWidths.context}
                            />
                            <Column
                                header={<Cell>Query</Cell>}
                                cell={({rowIndex}) => <Cell> {this.renderQuery(rowIndex)} </Cell>}
                                width={columnWidths.query}
                            />
                            <Column
                                header={<Cell>Delete</Cell>}
                                cell={({rowIndex}) => <Cell>
                                    <button className="collection-btn" id={rowIndex} onClick={this.handleDeleteButton}>
                                        <i className="icon icon-center fa fa-trash-o" />
                                    </button>
                                </Cell>}
                                width={columnWidths.delete}
                            />
                        </Table>
                        <Pagination
                            defaultCurrent={1}
                            current={currentPage}
                            total={total}
                            defaultPageSize={10}
                            pageSize={pageSize}
                            onChange={this.onPageChange}
                            showTotal={total => `Total ${total} queries`}
                            style={{marginTop: 10}}
                            showSizeChanger
                            onShowSizeChange={this.onPageSizeChange}
                            selectComponentClass={Select}
                        />
                    </div>
                </div>
            )
        }
    }

    renderEmptyMessage() {
        return (
            <p className="info panel-body text-light text-center">No queries to show</p>
        );
    }
}

function selectQuery(query, sessionContext, e) {
    e.preventDefault();
    QueryActions.selectQuery({query, sessionContext});
}

export default CollectionTable;