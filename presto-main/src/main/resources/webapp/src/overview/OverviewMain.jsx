/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import  React from "react";
import EchartPart from "./EchartPart";
import Header from '../queryeditor/components/Header';
import Footer from "../queryeditor/components/Footer";
import OverviewStore from "./OverviewStore";
import {formatDataSizeBytes} from "../utils";
import NavigationMenu from "../NavigationMenu";

class OverviewMain extends React.Component{
    constructor(props) {
        super(props);
        this.state={
            totalNodes:0,
            totalMemory:0,
            memoryUsed:0,
            processCpuLoad:0,
            systemCpuLoad:0,
            tableData:[]
        }
        this._onChange=this._onChange.bind(this);
    }
    componentDidMount() {
        OverviewStore.listen(this._onChange);
    }

    _onChange(data){
        let table=[];
        if(data.memoryData){
            Object.keys(data.memoryData).map(key => {
                let obj = {};
                obj.id = key.slice(0, key.indexOf(" "));
                obj.ip = key.slice(key.indexOf("[") + 1, key.indexOf("]"))
                obj.count = data.memoryData[key].availableProcessors;
                let totalMemory = data.memoryData[key].totalNodeMemory.slice(0, -1);
                obj.nodeMemory = Number(totalMemory);
                obj.freeMemory = Number(totalMemory);
                if (typeof (data.memoryData[key].pools.general) != "undefined"){
                    obj.freeMemory = 0;
                    obj.freeMemory += data.memoryData[key].pools.general.freeBytes;
                    if (typeof (data.memoryData[key].pools.reserved) != "undefined"){
                        obj.freeMemory += data.memoryData[key].pools.reserved.freeBytes;
                    }
                }
                table.push(obj);
            })
        }
        if (typeof (data.memoryData) != "undefined" && typeof (data.lineData) != "undefined") {
            this.setState({
                totalNodes: data.memoryData ? Object.keys(data.memoryData).length : '',
                totalMemory: data.lineData.totalMemory,
                memoryUsed: data.lineData.reservedMemory,
                processCpuLoad: (data.lineData.processCpuLoad * 100).toFixed(2) + '%',
                systemCpuLoad: (data.lineData.systemCpuLoad * 100).toFixed(2) + '%',
                tableData: table
            })
        }
    }

    render() {
        return(
            <div>
                <div className='flex flex-row flex-initial header'>
                    <Header />
                </div>
                <div className='overview'>
                    <NavigationMenu active={"metrics"}/>
                    <div className="line-right">
                        <h2>Overview Dashboard</h2>
                        <div className="line-show">
                            <div className="line-part">
                                <EchartPart />
                            </div>
                            <div className="summary-info">
                                <div className="summary-detail">
                                    <h3>Summary Info</h3>
                                    <table style={{width: "100%", marginTop:"20px"}}>
                                        <tbody>
                                        <tr className="border-bottom">
                                            <td><p className="font-18">Total Nodes <a href="./nodes.html">(view list)</a></p></td><td><span className="float-right color-2b610a">{this.state.totalNodes}</span></td>
                                        </tr>
                                        <tr className="border-bottom">
                                            <td><p className="font-18 padding-top-10">Total Usable Memory</p></td><td><span className="float-right color-2b610a">{formatDataSizeBytes(this.state.totalMemory)}</span></td>
                                        </tr>
                                        <tr className="border-bottom">
                                            <td><p className="font-18 padding-top-10">Memory Used</p></td><td><span className="float-right color-2b610a">{formatDataSizeBytes(this.state.memoryUsed)}</span></td>
                                        </tr>
                                        <tr className="border-bottom">
                                            <td><p className="font-18 padding-top-10">Process CPU Load</p></td><td><span className="float-right color-2b610a">{this.state.processCpuLoad}</span></td>
                                        </tr>
                                        <tr>
                                            <td><p className="font-18 padding-top-10">System CPU Load </p></td><td><span className="float-right color-2b610a">{this.state.systemCpuLoad}</span></td>
                                        </tr>
                                        </tbody>
                                    </table>
                                    {/*<div className="border-bottom">*/}
                                    {/*    <p className="font-18">Total Nodes <a href="./nodes.html">(view list)</a><span className="float-right color-2b610a">{this.state.totalNodes}</span></p>*/}
                                    {/*</div>*/}
                                    {/*<div className="border-bottom">*/}
                                    {/*    <p className="font-18 padding-top-10">Total Usable Memory <span className="float-right color-2b610a">{formatDataSizeBytes(this.state.totalMemory)}</span></p>*/}
                                    {/*</div>*/}
                                    {/*<div className="border-bottom">*/}
                                    {/*    <p className="font-18 padding-top-10">Memory Used <span className="float-right color-2b610a">{formatDataSizeBytes(this.state.memoryUsed)}</span></p>*/}
                                    {/*</div>*/}
                                    {/*<div className="border-bottom">*/}
                                    {/*    <p className="font-18 padding-top-10">Process CPU Load <span className="float-right color-2b610a">{this.state.processCpuLoad}</span></p>*/}
                                    {/*</div>*/}
                                    {/*<div>*/}
                                    {/*    <p className="font-18 padding-top-10">System CPU Load <span className="float-right color-2b610a">{this.state.systemCpuLoad}</span></p>*/}
                                    {/*</div>*/}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className='flex flex-row flex-initial footer'>
                    <Footer/>
                </div>
            </div>
        )
    }
}

export default OverviewMain;