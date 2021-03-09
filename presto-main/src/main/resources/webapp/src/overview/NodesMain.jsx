/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import Header from '../queryeditor/components/Header';
import Footer from "../queryeditor/components/Footer";
import StatusFooter from "../queryeditor/components/StatusFooter";
import { formatDataSizeBytes } from "../utils";
import NavigationMenu from "../NavigationMenu";
import OverviewStore from "../overview/OverviewStore";
import OverviewActions from "./OverviewActions";

class NodesMain extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            tableData: [],
        }
        this._onChange = this._onChange.bind(this);
        this.lineDatas = this.lineDatas.bind(this);
    }
    componentDidMount() {
        OverviewStore.listen(this._onChange);
        this.lineDatas();
    }

    componentWillUnmount() {
        OverviewStore.unlisten(this._onChange);
        clearInterval(this.state.timer);
    }

    //obtained data per sec
    lineDatas() {
        this.state.timer = setInterval(() => {
            OverviewActions.getData();
            OverviewActions.getMemoryData();
        }, 1000)
    }

    _onChange(data) {
        let table = [];
        if (data.memoryData) {
            Object.keys(data.memoryData).map(key => {
                let obj = {};
                obj.id = key.slice(0, key.indexOf(" "));
                obj.ip = key.slice(key.indexOf("[") + 1, key.indexOf("]"))
                obj.role = key.slice(key.indexOf("]") + 2);
                obj.count = data.memoryData[key].availableProcessors;
                let totalMemory = data.memoryData[key].totalNodeMemory.slice(0, -1);
                obj.nodeMemory = totalMemory;
                obj.freeMemory = data.memoryData[key].pools.general.freeBytes + (data.memoryData[key].pools.reserved ? data.memoryData[key].pools.reserved.freeBytes : 0);
                obj.usedMemory = data.memoryData[key].pools.general.reservedBytes + (data.memoryData[key].pools.reserved ? data.memoryData[key].pools.reserved.reservedBytes : 0);
                obj.state = data.memoryData[key].state;
                table.push(obj);
            })
        }
        this.setState({
            tableData: table
        })
    }

    render() {
        return (
            <div>
                <div className='flex flex-row flex-initial header'>
                    <Header />
                </div>
                <div className='nodes'>
                    <NavigationMenu active={"nodes"} />
                    <div className="line-right">
                        <div className="line-show">
                            <div className="summary-table">
                                <h3>Cluster Nodes</h3>
                                <table className="table">
                                    <thead>
                                        <tr>
                                            <th>ID</th>
                                            <th>IP</th>
                                            <th>Role</th>
                                            <th>CPU Count</th>
                                            <th>Usable Node Memory</th>
                                            <th>Used Memory</th>
                                            <th>Free Memory</th>
                                            <th>State</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {this.state.tableData.map((ele, index) => (
                                            <tr key={index}>
                                                <td>{ele.id}</td>
                                                <td>{ele.ip}</td>
                                                <td>{ele.role}</td>
                                                <td>{ele.count}</td>
                                                <td>{formatDataSizeBytes(ele.nodeMemory)}</td>
                                                <td>{formatDataSizeBytes(ele.usedMemory)}</td>
                                                <td>{formatDataSizeBytes(ele.freeMemory)}</td>
                                                <td>{ele.state}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
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
        )
    }
}

export default NodesMain;
