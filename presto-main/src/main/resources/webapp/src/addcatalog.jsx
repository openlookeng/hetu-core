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
import ReactDOM from "react-dom";
import AddCatalog from "./queryeditor/components/AddCatalog.jsx";
import ModalDialog from "./queryeditor/components/ModalDialog";

export default class AddCatalogContainer extends React.Component {
    constructor(props) {
        super();
        this.state = {
            show: false
        };
        this.showModal = this.showModal.bind(this);
    }

    showModal(e) {
        let newState = this.state;
        newState.show = !this.state.show;
        this.setState(newState);
    }

    render() {
        return (
            <div style={this.props.style} className={this.props.className}>
                <button className={"btn btn-success btn-lg active addcatalog"} style={{margin:'10px'}}
                        onClick={this.showModal.bind(this)}>Add Catalog</button>
                <ModalDialog onClose={this.showModal} header={"Add Catalog"} footer={""}
                             show={this.state.show}>
                    <AddCatalog onClose={this.showModal.bind(this)} refreshCallback={this.props.refreshCallback}/>
                </ModalDialog>
            </div>
        );
    }
}