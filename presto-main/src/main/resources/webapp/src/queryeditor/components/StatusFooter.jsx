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
import {addExponentiallyWeightedToHistory, addToHistory, formatCount, formatDataSizeBytes, precisionRound, percentage } from "../../utils";

const SPARKLINE_PROPERTIES = {
    width:'4.5vw',
    height:'25px',
    fillColor: '',
    //fillOpacity: .8,
    lineColor: '#000',
    //spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
    spotColor: '',
    highlightSpotColor: '',
    highlightLineColor: '',
    minSpotColor: '',
    maxSpotColor: '',
};

class StatusFooter
    extends React.Component {
    
        constructor(props) {
            super(props);
            this.state = {
                runningQueries: [],
                queuedQueries: [],
                blockedQueries: [],
                activeWorkers: [],
                runningDrivers: [],
                reservedMemory: [],
                totalMemory : 0,
                cpuUsage:[],
                rowInputRate: [],
                byteInputRate: [],
                perWorkerCpuTimeRate: [],
    
                lastRender: null,
                lastRefresh: null,
    
                lastInputRows: null,
                lastInputBytes: null,
                lastCpuTime: null,
    
                initialized: false,
            };
    
            this.refreshLoop = this.refreshLoop.bind(this);
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
            $.get('../v1/cluster', function (clusterState) {
    
                let newPerWorkerCpuTimeRate = [];
                if (this.state.lastRefresh !== null) {
                    const cpuTimeSinceRefresh = clusterState.totalCpuTimeSecs - this.state.lastCpuTime;
                    const secsSinceRefresh = (Date.now() - this.state.lastRefresh) / 1000.0;
    
                    newPerWorkerCpuTimeRate = addExponentiallyWeightedToHistory((cpuTimeSinceRefresh / clusterState.activeWorkers) / secsSinceRefresh, this.state.perWorkerCpuTimeRate);
                }
    
                this.setState({
                    // instantaneous stats
                    runningQueries: addToHistory(clusterState.runningQueries, this.state.runningQueries),
                    queuedQueries: addToHistory(clusterState.queuedQueries, this.state.queuedQueries),
                    blockedQueries: addToHistory(clusterState.blockedQueries, this.state.blockedQueries),
                    activeWorkers: addToHistory(clusterState.activeWorkers, this.state.activeWorkers),
    
                    // moving averages
                    runningDrivers: addExponentiallyWeightedToHistory(clusterState.runningDrivers, this.state.runningDrivers),
                    reservedMemory: addExponentiallyWeightedToHistory(clusterState.reservedMemory, this.state.reservedMemory),
                    cpuUsage: addExponentiallyWeightedToHistory(clusterState.systemCpuLoad * 100, this.state.cpuUsage),
                    totalMemory : clusterState.totalMemory,
                    perWorkerCpuTimeRate: newPerWorkerCpuTimeRate,
                    lastCpuTime: clusterState.totalCpuTimeSecs,
    
                    initialized: true,
    
                    lastRefresh: Date.now()
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

    componentDidUpdate() {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender === null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#running-queries-sparkline').sparkline(this.state.runningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#blocked-queries-sparkline').sparkline(this.state.blockedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#queued-queries-sparkline').sparkline(this.state.queuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));

            $('#active-workers-sparkline').sparkline(this.state.activeWorkers, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#running-drivers-sparkline').sparkline(this.state.runningDrivers, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));
            $('#cpu-usage-sparkline').sparkline(this.state.cpuUsage, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0, chartRangeMax:100, numberFormatter: precisionRound}));
            $('#reserved-memory-sparkline').sparkline(this.state.reservedMemory, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));
            $('#cpu-time-rate-sparkline').sparkline(this.state.perWorkerCpuTimeRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));

            this.setState({
                lastRender: renderTimestamp
            });
        }
        $('[data-toggle="tooltip"]').tooltip();
    }


    render() {
        return (
            <div className='flex'>
                <div className='flex flex-initial'>
                    <div className="clusterOverview">
                        <div className="clusterOverviewIconSetContainer" style={{minWidth:"60px"}} data-toggle="tooltip" data-placement="top" title="Active Workers">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-person-check-fill" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M1 14s-1 0-1-1 1-4 6-4 6 3 6 4-1 1-1 1H1zm5-6a3 3 0 1 0 0-6 3 3 0 0 0 0 6zm9.854-2.854a.5.5 0 0 1 0 .708l-3 3a.5.5 0 0 1-.708 0l-1.5-1.5a.5.5 0 0 1 .708-.708L12.5 7.793l2.646-2.647a.5.5 0 0 1 .708 0z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {this.state.activeWorkers[this.state.activeWorkers.length - 1]} </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Avg Cluster Cpu Usage">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-cpu-fill cpuIco" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M5.5.5a.5.5 0 0 0-1 0V2A2.5 2.5 0 0 0 2 4.5H.5a.5.5 0 0 0 0 1H2v1H.5a.5.5 0 0 0 0 1H2v1H.5a.5.5 0 0 0 0 1H2v1H.5a.5.5 0 0 0 0 1H2A2.5 2.5 0 0 0 4.5 14v1.5a.5.5 0 0 0 1 0V14h1v1.5a.5.5 0 0 0 1 0V14h1v1.5a.5.5 0 0 0 1 0V14h1v1.5a.5.5 0 0 0 1 0V14a2.5 2.5 0 0 0 2.5-2.5h1.5a.5.5 0 0 0 0-1H14v-1h1.5a.5.5 0 0 0 0-1H14v-1h1.5a.5.5 0 0 0 0-1H14v-1h1.5a.5.5 0 0 0 0-1H14A2.5 2.5 0 0 0 11.5 2V.5a.5.5 0 0 0-1 0V2h-1V.5a.5.5 0 0 0-1 0V2h-1V.5a.5.5 0 0 0-1 0V2h-1V.5zm1 4.5A1.5 1.5 0 0 0 5 6.5v3A1.5 1.5 0 0 0 6.5 11h3A1.5 1.5 0 0 0 11 9.5v-3A1.5 1.5 0 0 0 9.5 5h-3zm0 1a.5.5 0 0 0-.5.5v3a.5.5 0 0 0 .5.5h3a.5.5 0 0 0 .5-.5v-3a.5.5 0 0 0-.5-.5h-3z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent">{formatCount(this.state.cpuUsage[this.state.cpuUsage.length - 1])}%</div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="cpu-usage-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" style={{minWidth:"calc(10vw + 60px)"}} data-toggle="tooltip" data-placement="top" title="Used Query Memory">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-grid-3x2-gap-fill ramIco" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path d="M1 4a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1H2a1 1 0 0 1-1-1V4zm5 0a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1V4zm5 0a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1V4zM1 9a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1H2a1 1 0 0 1-1-1V9zm5 0a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1V9zm5 0a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2a1 1 0 0 1-1 1h-2a1 1 0 0 1-1-1V9z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"  style={{minWidth:"100px", textAlign:"center"}}>{formatDataSizeBytes(this.state.reservedMemory[this.state.reservedMemory.length - 1])}<span className="seprator">/</span>{formatDataSizeBytes(this.state.totalMemory)}</div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="reserved-memory-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Running Queries">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-list-check" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M5 11.5a.5.5 0 0 1 .5-.5h9a.5.5 0 0 1 0 1h-9a.5.5 0 0 1-.5-.5zm0-4a.5.5 0 0 1 .5-.5h9a.5.5 0 0 1 0 1h-9a.5.5 0 0 1-.5-.5zm0-4a.5.5 0 0 1 .5-.5h9a.5.5 0 0 1 0 1h-9a.5.5 0 0 1-.5-.5zM3.854 2.146a.5.5 0 0 1 0 .708l-1.5 1.5a.5.5 0 0 1-.708 0l-.5-.5a.5.5 0 1 1 .708-.708L2 3.293l1.146-1.147a.5.5 0 0 1 .708 0zm0 4a.5.5 0 0 1 0 .708l-1.5 1.5a.5.5 0 0 1-.708 0l-.5-.5a.5.5 0 1 1 .708-.708L2 7.293l1.146-1.147a.5.5 0 0 1 .708 0zm0 4a.5.5 0 0 1 0 .708l-1.5 1.5a.5.5 0 0 1-.708 0l-.5-.5a.5.5 0 0 1 .708-.708l.146.147 1.146-1.147a.5.5 0 0 1 .708 0z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {this.state.runningQueries[this.state.runningQueries.length - 1]} </div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="running-queries-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Queued Queries">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-person-lines-fill" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M1 14s-1 0-1-1 1-4 6-4 6 3 6 4-1 1-1 1H1zm5-6a3 3 0 1 0 0-6 3 3 0 0 0 0 6zm7 1.5a.5.5 0 0 1 .5-.5h2a.5.5 0 0 1 0 1h-2a.5.5 0 0 1-.5-.5zm-2-3a.5.5 0 0 1 .5-.5h4a.5.5 0 0 1 0 1h-4a.5.5 0 0 1-.5-.5zm0-3a.5.5 0 0 1 .5-.5h4a.5.5 0 0 1 0 1h-4a.5.5 0 0 1-.5-.5zm2 9a.5.5 0 0 1 .5-.5h2a.5.5 0 0 1 0 1h-2a.5.5 0 0 1-.5-.5z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {this.state.queuedQueries[this.state.queuedQueries.length - 1]} </div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="queued-queries-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Blocked Queries">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-slash-circle blockIco" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M8 15A7 7 0 1 0 8 1a7 7 0 0 0 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z" />
                                    <path fillRule="evenodd" d="M11.854 4.146a.5.5 0 0 1 0 .708l-7 7a.5.5 0 0 1-.708-.708l7-7a.5.5 0 0 1 .708 0z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {this.state.blockedQueries[this.state.blockedQueries.length - 1]} </div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="blocked-queries-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Avg Running Tasks">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-hdd-fill diskIco" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M0 10a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-1zm2.5 1a.5.5 0 1 0 0-1 .5.5 0 0 0 0 1zm2 0a.5.5 0 1 0 0-1 .5.5 0 0 0 0 1z" />
                                    <path d="M.91 7.204A2.993 2.993 0 0 1 2 7h12c.384 0 .752.072 1.09.204l-1.867-3.422A1.5 1.5 0 0 0 11.906 3H4.094a1.5 1.5 0 0 0-1.317.782L.91 7.204z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {formatCount(this.state.runningDrivers[this.state.runningDrivers.length - 1])} </div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="running-drivers-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                        <div className="clusterOverviewIconSetContainer" data-toggle="tooltip" data-placement="top" title="Avg CPU Cycles per Worker">
                            <div className="clusterOverviewIcon">
                                <svg width="1em" height="1em" viewBox="0 0 16 16" className="bi bi-list" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                                    <path fillRule="evenodd" d="M2.5 11.5A.5.5 0 0 1 3 11h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5zm0-4A.5.5 0 0 1 3 7h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5zm0-4A.5.5 0 0 1 3 3h10a.5.5 0 0 1 0 1H3a.5.5 0 0 1-.5-.5z" />
                                </svg>
                            </div>
                            <div className="clusterOverviewContent"> {formatCount(this.state.perWorkerCpuTimeRate[this.state.perWorkerCpuTimeRate.length - 1])} </div>
                            <div className="clusterOverviewGraph">
                                <span className="sparkline" id="cpu-time-rate-sparkline"><div className="loader">Loading ...</div></span>
                            </div>
                        </div>

                    </div>
                </div>
            </div>
        );
    }
}

export default StatusFooter;