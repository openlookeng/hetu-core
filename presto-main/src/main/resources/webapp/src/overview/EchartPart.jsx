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
import echarts from 'echarts/lib/echarts';
import "echarts/lib/chart/line";
import "echarts/lib/chart/treemap";
import "echarts/theme/royal";
import "echarts/lib/component/tooltip";
import "echarts/lib/component/title";
import OverviewActions from "./OverviewActions";
import OverviewStore from "./OverviewStore";
import MultiSelect from 'react-simple-multi-select';
import {formatCount, formatDataSizeBytes, bubbleSort} from "../utils";
import _ from "lodash";

class EchartPart extends React.Component{
    constructor(props) {
        super(props);
        this.state={
            checkStatus:{
                checkOne:true,
                checkTwo:true,
                checkThree:true,
                checkFour:true,
                checkFive:true,
                checkSix:true,
                checkSeven:true,
                heatMapChart: true,
                cpuLoad: true,
                heatMapMemoryChart: true
            },
            itemList: [
                {key: "Cluster CPU Usage", value: "heatMapChart"},
                {key: "Cluster Free Memory", value: "heatMapMemoryChart"},
                {key: "Avg Cluster CPU Usage", value: "cpuLoad"},
                {key: "Used Query Memory", value: "checkOne"},
                {key: "Running Queries", value: "checkTwo"},
                {key: "Queued Queries", value: "checkThree"},
                {key: "Blocked Queries", value: "checkFour"},
                {key: "Active Workers", value: "checkFive"},
                {key: "Avg Running Tasks", value: "checkSix"},
                {key: "Avg CPU cycles per worker", value: "checkSeven"}
              ],
            selectedItemList: [
                {key: "Cluster CPU Usage", value: "heatMapChart"},
                {key: "Cluster Free Memory", value: "heatMapMemoryChart"},
                {key: "Avg Cluster CPU Usage", value: "cpuLoad"},
                {key: "Used Query Memory", value: "checkOne"},
                {key: "Running Queries", value: "checkTwo"},
                {key: "Queued Queries", value: "checkThree"},
                {key: "Blocked Queries", value: "checkFour"},
                {key: "Active Workers", value: "checkFive"},
                {key: "Avg Running Tasks", value: "checkSix"},
                {key: "Avg CPU cycles per worker", value: "checkSeven"}
              ],
            chartName:['Used Query Memory', 'Running Queries', 'Queued Queries', 'Blocked Queries', 'Active Workers', 'Avg Running Tasks', 'Avg CPU cycles per worker'],
            step:10,
            timer:null,
            chartCpu:[],
            heatMapChart: [],
            heatMapMemoryChart: [],
            chart1:[],
            chart2:[],
            chart3:[],
            chart4:[],
            chart5:[],
            chart6:[],
            chart7:[],
            chartRef:null,
            lastRow:null,
            lastByte:null,
            lastWorker:null,
            memoryInit:false,
            unitArr:['bytes','quantity','quantity','quantity','quantity','quantity','quantity'],
            lastRefresh: null
        };
        this.state.chartRef = Object.keys(this.state.checkStatus),
        this._onChange=this._onChange.bind(this);
        this.changeList = this.changeList.bind(this);
        this.resize = this.resize.bind(this);
    }

    resize() {
        for (let i = 0; i < this.state.chartRef.length; i++) {
            let ref = this.refs[this.state.chartRef[i]];
            if (!ref.className) {
                let chart = echarts.init(ref);
                chart.resize({silent: true})
            }
        }
    }

    changeList(selectedItemList) {
        this.state.itemList.map(item => {this.state.checkStatus[item.value]=false})
        selectedItemList.map(item => {this.state.checkStatus[item.value]=true})
        let state = this.state;
        state.selectedItemList = selectedItemList;
        this.setState(state);
    }

    changeState(name){
        let state = this.state;
        state.checkStatus[name] = !state.checkStatus[name];
        this.setState(state);
    }

    //echarts
    componentDidMount() {
        this.setXAxis();
        OverviewActions.getData();
        OverviewActions.getMemoryData();
        OverviewStore.listen(this._onChange);
        this.lineDatas();

        let win = window;
        if (win.addEventListener) {
            win.addEventListener('resize', this.resize, false);
        } else if (win.attachEvent) {
            win.attachEvent('onresize', this.resize);
        } else {
            win.onresize = this.resize;
        }
        $(window).on('resize', this.resize);
    }

    componentWillUnmount() {
        OverviewStore.unlisten(this._onChange);
        clearInterval(this.state.timer);
    }

    //obtained data per sec
    lineDatas(){
        this.state.timer=setInterval(()=>{
            OverviewActions.getData();
            OverviewActions.getMemoryData();
        },1000)
    }
    //refresh line
    _onChange(data){
        if(data.requestNum%2===0){
            if(!this.state.memoryInit && data.memoryData){
            //     let cpuChart=echarts.init(this.refs.cpuLoad);
            //     let option=cpuChart.getOption();
            //     let memoryInitData=[];
            //     let cpuSeries={};
            //     let index = 0;
            //     Object.keys(data.memoryData).map(key=>{
            //         let op = Object.assign({}, option.series[index]);
            //         index++;
            //         op.name = key.slice(0, key.indexOf(" "));
            //         let currentCpuData = [...this.delete(this.state.chartCpu), [new Date().format('yyyy-MM-dd hh:mm:ss'), (data.memoryData[key].processCpuLoad * 100).toFixed(2)]];
            //         op.data = this.state.step === 10 ? currentCpuData.slice(1200) : this.state.step === 20 ? currentCpuData.slice(600) : currentCpuData;
            //         op.areaStyle = {
            //         shadowBlur: 10,
            //         opacity: 0.1
            //     };
            //     op.type = 'line';
            //     op.showSymbol = false;
            //     memoryInitData.push(op);
            //     cpuSeries[key]= currentCpuData;
            // })
            // option.series=memoryInitData;
            // option.yAxis = {max: 100, min: 0, type: "value"};
            // cpuChart.setOption(option);

                let heatMapChart = echarts.init(this.refs.heatMapChart, "royal");
                heatMapChart.setOption({
                    animation: false,
                    title: {
                        text: 'Cluster CPU Usage',
                        left: 'center',
                        textStyle: {
                            color: "#767676",
                            fontSize: 16
                        }
                    },
                    tooltip:{
                        trigger:'item',
                        formatter: function (params, t, cb) {
                            return params.name + " : " + params.value+"%";
                        }
                    },
                    series: [{
                        type: 'treemap',
                        data: this.state.heatMapChart
                    }]
                })
                let heatMapMemoryChart = echarts.init(this.refs.heatMapMemoryChart, "royal");
                heatMapMemoryChart.setOption({
                    animation: false,
                    title: {
                        text: 'Cluster Free Memory ',
                        left: 'center',
                        textStyle: {
                            color: "#767676",
                            fontSize: 16
                        }
                    },
                    tooltip:{
                        trigger:'item',
                        formatter: function (params, t, cb) {
                            return params.name + " : " + formatDataSizeBytes(params.value);
                        }
                    },
                    series: [{
                        type: 'treemap',
                        data: this.state.heatMapMemoryChart
                    }]
                })

                this.setState({
                    memoryInit:true
                })
            }
            // else{
            //     let dataCpu=this.state.chartCpu;
            //     let  mychart1=echarts.init(this.refs.cpuLoad);
            //     let option=mychart1.getOption();
            //     let memoryInitData=option.series;
            //     Object.keys(data.memoryData).map(key=>{
            //         let dataCpuElement = dataCpu[key];
            //         if (_.isUndefined(dataCpuElement)) {
            //             let op = Object.assign({}, option.series[index]);
            //             op.name = key.slice(0, key.indexOf(" "));
            //             op.areaStyle = {
            //                 shadowBlur: 10,
            //                 opacity: 0.1
            //             };
            //             op.type = 'line';
            //             dataCpu[key] = [...this.delete(dataCpuElement), [new Date().format('yyyy-MM-dd hh:mm:ss'), (data.memoryData[key].processCpuLoad * 100).toFixed(2)]];
            //             op.data = dataCpu[key];
            //             memoryInitData.push(op);
            //         }
            //         else {
            //             dataCpu[key] = [...this.delete(dataCpuElement), [new Date().format('yyyy-MM-dd hh:mm:ss'), (data.memoryData[key].processCpuLoad * 100).toFixed(2)]];
            //         }
            //         for(let i=0,len=memoryInitData.length;i<len;i++){
            //             if(memoryInitData[i].name===key.slice(0,key.indexOf(" "))){
            //                 memoryInitData[i].data=this.state.step===10 ? dataCpu[key].slice(1200):this.state.step===20 ? dataCpu[key].slice(600):dataCpu[key];
            //             }
            //         }
            //     })
            //     option.series=memoryInitData;
            //     mychart1.setOption(option);
            //     this.setState({
            //         chartCpu:dataCpu
            //     })
            // }
            //heatMap data
            let heatMapData = this.state.heatMapChart;
            Object.keys(data.memoryData).map(key => {
                let id = data.memoryData[key].id;
                let name = key;
                let index = _.findIndex(heatMapData, {id: id});
                let newDataPoint = Number((data.memoryData[key].systemCpuLoad * 100).toFixed(2));
                if (index == -1) {
                    let newData = {};
                    newData.id = id;
                    newData.name = name;
                    newData.value = newDataPoint
                    newData.dataset = [newDataPoint];
                    newData.children = [];
                    heatMapData.push(newData);
                }
                else {
                    let entry = heatMapData[index];
                    let dataset = entry.dataset;
                    if (dataset.length >= 600) {
                        dataset = dataset.splice(600 - 1, dataset.length - 600 - 1);
                    }
                    dataset = [...dataset, newDataPoint]
                    entry.dataset = dataset;
                    let sum = 0;
                    for (let i = 0; i < dataset.length; i++) {
                        sum += dataset[i];
                    }
                    entry.value = Number((sum / dataset.length).toFixed(2));
                }
            });

            let heatMapDataSort = bubbleSort(heatMapData);
            this.state.heatMapChart = heatMapDataSort.slice(0,10);
            let heatMapChart = echarts.init(this.refs.heatMapChart, "royal");
            let heatMapChartOption = heatMapChart.getOption();
            heatMapChartOption.series = [{
                type: "treemap",
                data: heatMapDataSort.slice(0,10),
                breadcrumb: {
                    show: false
                }
            }];
            heatMapChart.setOption(heatMapChartOption);

            //heatMap memory data
            let heatMapMemoryData = this.state.heatMapMemoryChart;
            Object.keys(data.memoryData).map(key => {
                let id = data.memoryData[key].id;
                let name = key;
                let index = _.findIndex(heatMapMemoryData, {id: id});
                let newDataPoint = 0;
                if (typeof (data.memoryData[key].pools.general) != "undefined"){
                    newDataPoint += data.memoryData[key].pools.general.freeBytes;
                    if (typeof (data.memoryData[key].pools.reserved) != "undefined"){
                        newDataPoint += data.memoryData[key].pools.reserved.freeBytes;
                    }
                }
                newDataPoint = Number(newDataPoint);
                if (index == -1) {
                    let newData = {};
                    newData.id = id;
                    newData.name = name;
                    newData.value = newDataPoint
                    newData.dataset = [newDataPoint];
                    newData.children = [];
                    heatMapMemoryData.push(newData);
                }
                else {
                    let entry = heatMapMemoryData[index];
                    let dataset = entry.dataset;
                    if (dataset.length >= 600) {
                        dataset = dataset.splice(600 - 1, dataset.length - 600 - 1);
                    }
                    dataset = [...dataset, newDataPoint]
                    entry.dataset = dataset;
                    let sum = 0;
                    for (let i = 0; i < dataset.length; i++) {
                        sum += dataset[i];
                    }
                    entry.value = Number((sum / dataset.length).toFixed(2));
                }
            });
            let heatMapMemoryDataSort = bubbleSort(heatMapMemoryData);
            this.state.heatMapMemoryChart = heatMapMemoryDataSort.slice(0,10);
            let heatMapMemoryChart = echarts.init(this.refs.heatMapMemoryChart, "royal");
            let heatMapMemoryChartOption = heatMapMemoryChart.getOption();
            heatMapMemoryChartOption.series = [{
                type: "treemap",
                data: heatMapMemoryDataSort.slice(0,10),
                breadcrumb: {
                    show: false
                },
            }];
            heatMapMemoryChart.setOption(heatMapMemoryChartOption);

            let now = Date.now();
            let secondsSinceLastRefresh = this.state.lastRefresh ? (now - this.state.lastRefresh) / 1000.0 : 1;
            secondsSinceLastRefresh = secondsSinceLastRefresh < 1 ? 1 : secondsSinceLastRefresh;
            let lastWorker = this.state.lastWorker ? (data.lineData.totalCpuTimeSecs - this.state.lastWorker) / data.lineData.activeWorkers / secondsSinceLastRefresh : 0;
            this.setState({
                chartCpu:[...this.delete(this.state.chartCpu),[new Date().format('yyyy-MM-dd hh:mm:ss'), (data.lineData.systemCpuLoad * 100).toFixed(4)]],
                chart1:[...this.delete(this.state.chart1),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.reservedMemory]],
                chart2:[...this.delete(this.state.chart2),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.runningQueries]],
                chart3:[...this.delete(this.state.chart3),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.queuedQueries]],
                chart4:[...this.delete(this.state.chart4),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.blockedQueries]],
                chart5:[...this.delete(this.state.chart5),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.activeWorkers]],
                chart6:[...this.delete(this.state.chart6),[new Date().format('yyyy-MM-dd hh:mm:ss'),data.lineData.runningDrivers]],
                chart7:[...this.delete(this.state.chart7),[new Date().format('yyyy-MM-dd hh:mm:ss'),lastWorker]],
                lastWorker:data.lineData.totalCpuTimeSecs,
                heatMapChart: this.state.heatMapChart,
                heatMapMemoryChart: this.state.heatMapMemoryChart,
                lastRefresh: now
            });
            if (!this.refs.cpuLoad.className) {
                let mychart = echarts.init(this.refs.cpuLoad);
                let option = mychart.getOption();
                option.series[0].data = this.state.step === 10 ? this.state.chartCpu.slice(1200) : this.state.step === 20 ? this.state.chartCpu.slice(600) : this.state.chartCpu;
                option.series[0].areaStyle = {
                    color: "#41BB04",
                    shadowBlur: 10,
                    opacity: 0.1
                };
                option.series[0].lineStyle = {color: "#137113"};
                option.series[0].itemStyle = {color: "#137113"};
                option.yAxis = {max: 100, min: 0, type: "value"};
                mychart.setOption(option);
            }
            for(let i=0;i<this.state.chartName.length;i++){
                if(!this.refs[this.state.chartRef[i]].className){
                    let  mychart=echarts.init(this.refs[this.state.chartRef[i]]);
                    let option=mychart.getOption();
                    option.series[0].data = this.state.step===10 ? this.state['chart'+parseInt(i+1)].slice(1200):this.state.step===20 ? this.state['chart'+parseInt(i+1)].slice(600):this.state['chart'+parseInt(i+1)];
                    option.series[0].areaStyle = {
                        color: "#c3c683",
                        shadowBlur: 10,
                        opacity: 0.1
                    };
                    option.series[0].lineStyle = {color: "#b6a019"};
                    option.series[0].itemStyle = {color: "#b6a019"};
                    mychart.setOption(option);
                }
            }
        }
    }

    // delete first data
    delete(arr){
        if (_.isUndefined(arr)) {
            return [];
        }
        arr.splice(0,1);
        return arr;
    }
    //according to step to set XAxis data
    setXAxis(){
        let arr = [];
        for(let i =0,len=30*60;i<len;i++){
            arr[i] = [new Date(new Date().getTime() - 1000 * i).format('yyyy-MM-dd hh:mm:ss'), 0];
        }
        arr=arr.reverse();
        this.setState({
            chartCpu:[...arr],
            chart1:[...arr],
            chart2:[...arr],
            chart3:[...arr],
            chart4:[...arr],
            chart5:[...arr],
            chart6:[...arr],
            chart7:[...arr],
        });
        let  mychart1=echarts.init(this.refs.cpuLoad);
        mychart1.setOption({
            animation: false,
            title:{text:'Average Cluster CPU Usage',
                left:'center',
                textStyle: {
                    color: "#767676",
                    fontSize: 16
                }
            },
            tooltip:{
                trigger:'axis'
            },
            xAxis:{
                type:'time',
                name:'time',
                interval:60*1000*this.state.step/10,
                boundaryGap: false,
                axisLabel:{
                    formatter:function(value,index){
                        if (index % 2 == 1) {
                            return "";
                        }
                        let date=new Date(value).format("yyyy-MM-dd hh:mm:ss");
                        return date.slice(11,16);
                    }
                }
            },
            yAxis:{
                name:'usage(%)',
                axisTick:{
                    show:false
                },
                axisLabel:{
                    formatter: function (value, index) {
                        if (index % 2 == 1) {
                            return "";
                        }
                        return value;
                    }
                }
            },
            series:[{
                type:'line',
                symbol:'none',
                data:[]
            }]
        })
        for(let i=0;i<this.state.chartName.length;i++){
            if(!this.refs[this.state.chartRef[i]].className){
                let  mychart=echarts.init(this.refs[this.state.chartRef[i]]);
                mychart.setOption({
                    animation: false,
                    title:{
                        text:this.state.chartName[i], 
                        left:'center', 
                        textStyle: {
                            color: "#767676",
                            fontSize: 16
                            }
                        },
                    tooltip:{
                        trigger:'axis'
                    },
                    xAxis:{
                        type:'time',
                        name:'time',
                        interval:60*1000*this.state.step/10,
                        boundaryGap: false,
                        axisLabel:{
                            formatter:function(value,index){
                                if (index % 2 == 1) {
                                    return "";
                                }
                                let date=new Date(value).format("yyyy-MM-dd hh:mm:ss");
                                return date.slice(11,16);
                            }
                        }
                    },
                    yAxis:{
                        name:this.state.unitArr[i],
                        axisTick:{
                            show:false
                        },
                        axisLabel:{
                            formatter: function (name, value, index) {
                                if (index % 2 == 1) {
                                    return "";
                                }
                                if (name === 'quantity') {
                                    return formatCount(value);
                                }
                                else if (name === 'bytes') {
                                    return formatDataSizeBytes(value);
                                }
                                else {
                                    return value;
                                }
                            }.bind(null, this.state.unitArr[i])
                        }
                    },
                    series:[{
                        type:'line',
                        symbol:'none',
                        data:this.state.step===10 ? this.state['chart'+parseInt(i+1)].slice(1200):this.state.step===20 ? this.state['chart'+parseInt(i+1)].slice(600):this.state['chart'+parseInt(i+1)]
                    }]
                })
            }
        }
    }

    selected(e){
        clearInterval(this.state.timer);
        e.preventDefault();
        let val=e.target.selectedIndex===0?10:e.target.selectedIndex===1?20:30;
        let state = this.state;
        state.step = val;
        this.setState(state);
        for(let i=0;i<this.state.chartName.length;i++){
            if(!this.refs[this.state.chartRef[i]].className){
                let  mychart=echarts.init(this.refs[this.state.chartRef[i]]);
                let option=mychart.getOption();
                option.xAxis[0].interval=60*1000*this.state.step/10;
                // option.series[0].data=[];
                mychart.setOption(option);
            }
        }
        let  mychart1=echarts.init(this.refs.cpuLoad);
        let option=mychart1.getOption();
        option.xAxis[0].interval=60*1000*this.state.step/10;
        mychart1.setOption(option);
        OverviewActions.getData();
        this.lineDatas();
    }

    render() {
        let style = {height: "30vh", width: "calc(40vw - 80px)", left: "center", top: "center"}
        return(
            <div>
                <div className="selectItemContainer">
                <div className="selectChart multiSelect">
                     <MultiSelect
                            title={"Select Chart"}
                            itemList={this.state.itemList}
                            selectedItemList={this.state.selectedItemList}
                            changeList={this.changeList}
                            isObjectArray={true}
                      />
                </div>
                <div className="select-part">
                    <select onChange={this.selected.bind(this)} value={this.state.step}>
                        <option value="10">Last 10 minutes</option>
                        <option value="20">Last 20 minutes</option>
                        <option value="30">Last 30 minutes</option>
                    </select>
                </div>
                </div>
                <div className="overviewGraphContainerParent">
                <div className="overviewGraphContainer">
                    <div className={this.state.checkStatus["heatMapChart"] ? 'overviewChart' : 'display-none'}>
                        <div ref="heatMapChart" style={style}/>
                    </div>
                    <div className={this.state.checkStatus["heatMapMemoryChart"] ? 'overviewChart' : 'display-none'}>
                        <div ref="heatMapMemoryChart" style={style}/>
                    </div>
                    <div className={this.state.checkStatus["cpuLoad"] ? 'overviewChart' : 'display-none'}>
                        <div ref="cpuLoad" style={style}/>
                    </div>
                    {Object.keys(this.state.checkStatus).map((key, index) => {
                        if (key == 'cpuLoad' || key == 'heatMapChart' || key == 'heatMapMemoryChart') {
                            return null;
                        }
                        return (
                            <div className={this.state.checkStatus[key] ? 'overviewChart' : 'display-none'} key={index}>
                                <div ref={key} style={style}/>
                            </div>
                        )
                    })}
                </div>
                </div>
            </div>

        )
    }
}

export default EchartPart;