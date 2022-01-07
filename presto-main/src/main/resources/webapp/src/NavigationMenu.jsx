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

export default class NavigationMenu extends React.Component {

    constructor(args) {
        super(args);
    }

    render() {
        return (
            <div className="menu-left">
                <ul>
                    <li className={this.props.active === 'queryeditor' ? "active" : ""}>
                        <a href={this.props.active === 'queryeditor' ? "#" : "./queryeditor.html"}>
                            <div><i className="fa fa-home"></i></div>
                            <div>Home</div>
                        </a>
                    </li>
                    <li className={this.props.active === 'metrics' ? "active" : ""}>
                        <a href={this.props.active === 'metrics' ? "#" : "./overview.html"}>
                            <div><i className="fa fa-line-chart"></i></div>
                            <div>Metrics</div>
                        </a>
                    </li>
                    <li className={this.props.active === 'nodes' ? "active" : ""}>
                        <a href={this.props.active === 'nodes' ? "#" : "./nodes.html"}>
                            <div><i className="fa fa-server"></i></div>
                            <div>Nodes</div>
                        </a>
                    </li>
                    <li className={this.props.active === 'querymonitor' ? "active" : ""}>
                        <a href={this.props.active === 'querymonitor' ? "#" : "./querymonitor.html"}>
                            <div><i className="fa fa-desktop"></i></div>
                            <div>Query Monitor</div>
                        </a>
                    </li>
                    <li className={this.props.active === 'queryhistory' ? "active" : ""}>
                        <a href={this.props.active === 'queryhistory' ? "#" : "./queryhistory.html"}>
                            <div><i className="fa fa-history"></i></div>
                            <div>Query History</div>
                        </a>
                    </li>
                    <li className={this.props.active === 'auditlog' ? "active" : ""}>
                        <a href={this.props.active === 'auditlog' ? "#" : "./auditlog.html"}>
                            <div><i className="fa fa-file-text-o"></i></div>
                            <div>Audit Log</div>
                        </a>
                    </li>
                </ul>
            </div>
        );
    }
}