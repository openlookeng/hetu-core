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
import React from 'react';
import {Button, Col, Form, FormControl, FormGroup, FormLabel, Row} from "react-bootstrap";

const props = {};

class ShowCatalog extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            catalog_name:"",
            connection_password: "",
            connector_name: "",
            url: "",
            user: ""
        }
        this.showObj = {}
        this.handleClose = this.handleClose.bind(this);
        this.componentWillReceiveProps = this.componentWillReceiveProps.bind(this);
        this._objToStrMap = this._objToStrMap.bind(this);
        this.deleteMapKey = this.deleteMapKey.bind(this);
        this.renderCatalogProperties = this.renderCatalogProperties.bind(this);
    }

    componentWillReceiveProps(nextProps) {
        if (this.props.showText != nextProps.showText){
            this.state.catalog_name = nextProps.catalog_name;
            // this.state.connection_password = nextProps.connection_password;
            // this.state.connector_name = nextProps.connector_name;
            // this.state.url = nextProps.url;
            // this.state.user = nextProps.user;
            // this.show = nextProps;
            this.showObj = nextProps.showText
            this.setState();
            // this.setState({
            //     catalog_name : nextProps.catalog_name
            // })
        }
    }

    handleClose() {
        this.props.onClose && this.props.onClose();
    }

    _objToStrMap(obj){
        let strMap = new Map();
        for (let k of Object.keys(obj)) {
            strMap.set(k,obj[k]);
        }
        return strMap;
    }

    deleteMapKey(obj,key) {
        let newMap = new Map();
        for (let item of obj.keys()){
            if (item != key){
                newMap.set(item,obj.get(item));
            }
        }
        return newMap;
    }

    renderCatalogProperties(obj) {
        let arr = []
        for (let item of obj.keys()) {
            arr.push(
                <Row style={{display: "flex",alignItems:'center'}}>
                    <Col style={{flexDirection: "column", marginRight: '10px', width:"70%"}}>
                        <FormControl name="name" type="text" placeholder={"property name"}
                                     value={item}
                                     readOnly="true"/>
                    </Col>
                    <Col style={{flexDirection: "column", width:"100%"}}>
                        <FormControl name="value" type="text" placeholder={"property value"}
                                     value={obj.get(item)}
                                     readOnly="true"/>
                    </Col>
                </Row>
            )
        }
        return (
            arr
        )
    }

    render() {
        let showMap = this._objToStrMap(this.showObj);
        let catalogProperties = this.deleteMapKey(showMap,'connector.name');
        return(
            <div>
                <div className={"hetu-foem-body"} style={{position: "relative", paddingLeft: "20px", paddingRight: "20px", overflowY: "auto"}}>
                    <Form>
                        <FormGroup>
                            <Row>
                                <Col style={{display: "flex", alignItems:'center'}}>
                                    <FormLabel className={"hetu-form-label"}>Data Source Type</FormLabel>
                                </Col>
                                <Col>
                                    <div style={{display: "block"}}>
                                        <input className="form-control" value={showMap.get('connector.name')} readOnly="true"/>
                                    </div>
                                </Col>
                            </Row>
                            <Row>
                                <Col style={{display: "flex", alignItems:'center'}}>
                                    <FormLabel className={"hetu-form-label"}>Catalog Name</FormLabel>
                                </Col>
                                <Col>
                                    <div style={{display: "block"}}>
                                        <input className="form-control" value={this.state.catalog_name} disabled="true"/>
                                    </div>
                                </Col>
                            </Row>
                            {catalogProperties.size == 0 ?
                                null
                                :
                                <Row>
                                    <Col style={{display: "flex", alignItems: 'center'}}>
                                        <FormLabel className={"hetu-form-label"}>Catalog Properties</FormLabel>
                                    </Col>
                                    <Col>
                                        <div style={{display: "block"}}>
                                            {this.renderCatalogProperties(catalogProperties)}
                                        </div>
                                    </Col>
                                </Row>
                            }
                        </FormGroup>
                    </Form>
                </div>
                <div className={'catalog-btn-part'}>
                    <Button onClick={this.handleClose} className={"btn btn-lg"}>Close</Button>
                </div>
            </div>
        )
    }
}

export default ShowCatalog