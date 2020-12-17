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

export default class ModalDialog extends React.Component {

    constructor(props) {
        super(props);
        this.onClose = this.onClose.bind(this);
    }

    onClose(e) {
        this.props.onClose && this.props.onClose(e);
    }

    componentDidMount() {
        //Disable navogation using tab key.
        $(":input, a").removeAttr("tabindex");
    }

    componentWillUnmount() {

    }

    render() {
        if (!this.props.show) {
            return null;
        }
        return (
            <div className="hetu-modal-dialog">
                <div className="hetu-modal-dialog-content">
                    <div className="hetu-modal-dialog-header">
                        <span className="hetu-modal-dialog-close glyphicon glyphicon-remove" onClick={this.onClose}></span>
                        <h3>{this.props.header}</h3>
                    </div>
                    <div className={"hetu-modal-dialog-body"}>{this.props.children}</div>
                    <div className="hetu-hetu-modal-dialog-footer">
                        <h3>{this.props.footer}</h3>
                    </div>
                </div>
            </div>
        )
    }
}