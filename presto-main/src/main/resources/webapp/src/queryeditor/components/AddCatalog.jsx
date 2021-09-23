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
import React from 'react';
import {Button, Col, Form, FormControl, FormGroup, FormLabel} from "react-bootstrap";
import FormFileInput from "react-bootstrap/FormFileInput";
import Row from "react-bootstrap/Row";
import {Alert} from 'react-bootstrap/Alert'
import CatalogActions from "../actions/CatalogActions";
import ConnectorActions from "../actions/ConnectorActions";
import ConnectorStore from "../stores/ConnectorStore";

function getStateFromStore() {
    return {
        supportedConnectors: ConnectorStore.getCollection().all({
            sort: true
        })
    };
}

class AddCatalog extends React.Component {
    constructor(props) {
        super(props);

        this.state = this.getInitialState();
        this.handleSubmit = this.handleSubmit.bind(this);
        this.handleClose = this.handleClose.bind(this);
        this.handleChange = this.handleChange.bind(this);
        this.addProperty = this.addProperty.bind(this);
        this.addConfiguration = this.addConfiguration.bind(this);
        this.handleValidation = this.handleValidation.bind(this);
        this._fetchConnectors = this._fetchConnectors.bind(this);
        this._onChange = this._onChange.bind(this);
    }

    getInitialErrors() {
        return {
            catalogProperties: [],
            catalogConfigProperties: [],
            globalConfigProperties: []
        };
    }

    getInitialState() {
        return {
            supportedConnectors: ConnectorStore.getCollection().all({
                sort: true
            }),
            catalogName: "",
            connectorName: "",
            selectedConnector: "",
            catalogAreaProperties: "",
            catalogProperties: [],
            catalogConfigProperties: [],
            globalConfigProperties: [],
            errors: this.getInitialErrors()
        }
    }

    componentDidMount() {
        ConnectorStore.listen(this._onChange);
        this._fetchOriginalSupportedConnectors();
    }

    componentWillUnmount() {
        ConnectorStore.unlisten(this._onChange);
    }

    _fetchConnectors() {
        ConnectorActions.fetchSupportedConnectors();
    }

    _fetchOriginalSupportedConnectors() {
        ConnectorActions.fetchOriginalSupportedConnectors();
    }

    _onChange() {
        this.setState(getStateFromStore());
    }

    handleValidation() {
        let state = this.state;
        let errors = {
            catalogProperties: [],
            catalogConfigProperties: [],
            globalConfigProperties: []
        };
        let error = false;
        state.catalogName = state.catalogName.trim();
        if (state.catalogName == "") {
            errors.catalogName = "Catalog Name cannot be empty"
            error = true;
        }
        else if (!(state.catalogName.match("^([A-Za-z]+)([A-Za-z0-9_]*)([A-Za-z0-9]+)$"))) {
            errors.catalogName = "Catalog name must contain only alphanumeric and/or underscore(s). Must start with alphabet and end with alphanumeric character"
            error = true;
        }

        if (state.connectorName.trim() == "" || state.connectorName == "placeholder") {
            errors.connectorName = "Select a connector"
            error = true;
        }

        if (state.catalogProperties.length > 0) {
            for (const [index, entry] of state.catalogProperties.entries()) {
                let name = entry["name"];
                let value = entry["value"];
                name = name == undefined ? name : name.trim();
                value = value == undefined ? value : value.trim();

                if (name == "" || value == "") {
                    errors.catalogProperties.push("Property name and value cannot be empty")
                    error = true;
                }
                else if (entry["type"] === "files") {
                    let missedFiles = [];
                    let files = value.toString().split(",")
                    files.forEach((file, index, files) => {
                        let found = false;
                        Array.from(state.catalogConfigProperties).map((config) => {
                            if (config != undefined && config != null && config.name == file) {
                                found = true;
                            }
                        })
                        if (found) {
                            return;
                        }
                        Array.from(state.globalConfigProperties).map((config) => {
                            if (config != undefined && config != null && config.name == file) {
                                found = true;
                            }
                        })
                        if (!found) {
                            missedFiles.push(file);
                        }
                    });
                    if (missedFiles.length > 0) {
                        error = true;
                        errors.catalogProperties.push("Following files needs to be uploaded: [" + missedFiles.toString() + "]")
                    }
                    else {
                        errors.catalogProperties.push("")
                    }
                }
                else {
                    errors.catalogProperties.push("")
                }
            }
        }
        if (state.catalogConfigProperties.length > 0) {
            for (const [index, entry] of state.catalogConfigProperties.entries()) {
                if (entry === undefined || entry === null || entry == {}) {
                    errors.catalogConfigProperties.push("Specify catalog configuration file!!")
                    error = true;
                }
                else {
                    errors.catalogConfigProperties.push("")
                }
            }
        }
        if (state.globalConfigProperties.length > 0) {
            for (const [index, entry] of state.globalConfigProperties.entries()) {
                if (entry === undefined || entry === null || entry == {}) {
                    errors.globalConfigProperties.push("Specify Global configuration file!!")
                    error = true;
                }
                else {
                    errors.globalConfigProperties.push("");
                }
            }
        }
        state.errors = errors;
        this.setState(state);
        return !error;
    }

    handleChange(e) {
        let newState = this.state;
        if (["catalogName"].includes(e.target.name)) {
            newState.catalogName = e.target.value.toString().trim().toLowerCase();
            if (newState.catalogName != "" && newState.errors["catalogName"] != "") {
                newState.errors.catalogName = "";
            }
        }
        else if (["connectorName"].includes(e.target.name)) {
            newState.connectorName = e.target.value;
            newState.catalogProperties.splice(0, newState.catalogProperties.length);
            if (newState.connectorName != "" && newState.connectorName != "placeholder"
                && newState.errors["connectorName"] != undefined && newState.errors["connectorName"] != "") {
                newState.errors.connectorName = "";
            }
            for (const eachVal of newState.supportedConnectors) {
                if (eachVal.connectorWithProperties.connectorName === newState.connectorName) {
                    newState.selectedConnector = eachVal;
                    newState.errors.catalogProperties = [];
                    if (eachVal.connectorWithProperties.propertiesEnabled) {
                        for (const [index, value] of Object.entries(eachVal.connectorWithProperties.properties)) {
                            newState.catalogProperties.push(value);
                            newState.errors.catalogProperties.push("");
                        }
                    }
                }
            }
        }
        this.setState(newState);
    }

    addProperty(e) {
        e.preventDefault();
        let newState = this.state;
        newState.catalogProperties.push({
            name: "",
            value: ""
        });
        newState.errors.catalogProperties.push("");
        this.setState(newState);
    }

    handlePropertyChange(e) {
        if (["name", "value"].includes(e.target.name)) {
            let newState = this.state;
            newState.catalogProperties[e.target.dataset.id][e.target.name] = e.target.value;
            if (e.target.name != "" && e.target.value != ""
                && newState.errors.catalogProperties[e.target.dataset.id] != undefined
                && newState.errors.catalogProperties[e.target.dataset.id] != "") {
                newState.errors.catalogProperties[e.target.dataset.id] = "";
            }
            this.setState(newState);
        }
    }

    removeProperty(idx) {
        let newState = this.state;
        if (idx < newState.catalogProperties.length && idx > -1) {
            let removingProperty = newState.catalogProperties[idx];
            if (removingProperty["required"]) {
                if (!confirm("'" + removingProperty.name + "' is a required property.\n" +
                    "Are you sure to remove?")) {
                    return;
                }
            }
            newState.catalogProperties.splice(idx, 1);
            newState.errors.catalogProperties.splice(idx, 1);
        }
        this.setState(newState);
    }

    addConfiguration(e) {
        e.preventDefault();
        let newState = this.state;
        newState.catalogConfigProperties.push(null);
        newState.errors.catalogConfigProperties.push("");
        this.setState(newState);
    }

    addGlobalConfiguration(e){
        e.preventDefault();
        let newState = this.state;
        newState.globalConfigProperties.push(null);
        newState.errors.globalConfigProperties.push("");
        this.setState(newState);
    }

    removeConfiguration(e) {
        let newState = this.state;
        newState.catalogConfigProperties.splice(e.target.dataset.id, 1);
        this.setState(newState);
    }

    removeGlobalConfiguration(e) {
        let newState = this.state;
        newState.globalConfigProperties.splice(e.target.dataset.id, 1);
        this.setState(newState);
    }

    handleConfigFileChange(e) {
        let newState = this.state;
        if (["configfile"].includes(e.target.name)) {
            if (e.target.files.length > 0) {
                newState.catalogConfigProperties[e.target.dataset.id] = e.target.files[0];
                if (newState.errors.catalogConfigProperties[e.target.dataset.id] != undefined
                    && newState.errors.catalogConfigProperties[e.target.dataset.id] != "") {
                    newState.errors.catalogConfigProperties[e.target.dataset.id] = "";
                }
            }
            else {
                newState.catalogConfigProperties[e.target.dataset.id] = null;
                newState.errors.catalogConfigProperties[e.target.dataset.id] = "";
            }
            this.setState(newState);

        }
        else if (["global-configfile"].includes(e.target.name)) {
            if (e.target.files.length > 0) {
                newState.globalConfigProperties[e.target.dataset.id] = e.target.files[0];
                if (newState.errors.globalConfigProperties[e.target.dataset.id] != undefined
                    && newState.errors.globalConfigProperties[e.target.dataset.id] != "") {
                    newState.errors.globalConfigProperties[e.target.dataset.id] = "";
                }
            }
            else {
                newState.globalConfigProperties[e.target.dataset.id] = null;
                newState.errors.globalConfigProperties[e.target.dataset.id] = "";
            }
            this.setState(newState);
        }
    }

    handleSubmit(e) {
        e.preventDefault();
        if (!this.handleValidation()) {
            return;
        }
        let formData = new FormData();

        let props = {};
        Array.from(this.state.catalogProperties).map(row => {
            props[row.name] = row.value
        });
        formData.append("catalogInformation", JSON.stringify({
            catalogName: this.state.catalogName,
            connectorName: this.state.connectorName,
            properties: props,
        }))

        Array.from(this.state.catalogConfigProperties).map(entry => {
            if (entry != null) {
                formData.append("catalogConfigurationFiles", entry, entry.name);
            }
        });
        Array.from(this.state.globalConfigProperties).map(entry => {
            if (entry != null) {
                formData.append("globalConfigurationFiles", entry, entry.name);
            }
        });

        CatalogActions.addCatalog(formData).then((result) => {
            let newState = this.state;
            if (!result.result) {
                if(result.message.indexOf('Not Found (code: 404)') !==-1) {
                    newState.errors["submissionError"] = "Error while adding catalog: service is not available. probably because 'catalog.dynamic-enabled' in the config.properties is set to false.";
                    this.setState(newState)
                } else {
                    newState.errors["submissionError"] = "Error while adding catalog: " + result.message.split('\n', 1)[0];
                    this.setState(newState)
                }
            }
            else {
                newState.errors["submissionSuccess"] = "Add catalog successful; Server message: " + result.message
                if (this.props.refreshCallback != undefined) {
                    this.props.refreshCallback();
                }
                this.handleClose();
            }
        });
    }

    handleReset() {
        this.setState(this.getInitialState());
    }

    handleClose() {
        this.props.onClose && this.props.onClose();
    }

    render() {
        let {catalogProperties} = this.state;
        let {catalogConfigProperties} = this.state;
        let {globalConfigProperties} = this.state;
        return (
            <div>
                <div className={"hetu-form-body"} style={{position: "relative", paddingLeft: "20px", paddingRight: "20px", overflowY: "auto"}}>
                    <div style={{paddingLeft: "20px", paddingRight: "20px", paddingBottom: "10px"}}>
                        <span className={"hetu-form-submit-success"}>
                            {this.state.errors["submissionSuccess"]}
                        </span>
                        <span className={"hetu-form-submit-error"}>
                            {this.state.errors["submissionError"]}
                        </span>
                    </div>
                    <Form>
                        <FormGroup>
                            <Row>
                                <Col style={{display: "flex", alignItems:'center'}}>
                                    <FormLabel className={"hetu-form-label"}>Data Source Type</FormLabel>
                                    {
                                        (() => {
                                            if (this.state.selectedConnector == "") {
                                                return (
                                                    <i className="fa fa-info-circle fa-form-label"
                                                       title={"Select a Data source type(Connector) and click here for more details of selected Data source type(Connector)"}></i>)
                                            }
                                            else {
                                                return (
                                                    <a href={this.state.selectedConnector.docLink} className="alert-link" target="_blank">
                                                        <i className="fa fa-info-circle fa-form-label"
                                                           title={"Click for more details of current connector"}></i>
                                                    </a>
                                                );
                                            }
                                        })()
                                    }
                                </Col>
                                <Col>
                                    <div style={{display: "block"}}>
                                        {/*<FormControl name="connectorName" type="text" style={{flexDirection: "column"}} onChange={this.handleChange}*/}
                                        {/*             placeholder={"Connector Name"} value={this.state.connectorName}/>*/}
                                        <Form.Control as="select" size='lg' name="connectorName" defaultValue="placeholder" onChange={this.handleChange}>
                                            <option key={"placeholder"} value="placeholder" disabled={true}>Select a data source type</option>
                                            {
                                                this.state.supportedConnectors.map(eachVal =>
                                                    <option key={eachVal.connectorWithProperties.connectorName}
                                                            value={eachVal.connectorWithProperties.connectorName}>{eachVal.connectorWithProperties.connectorLabel}</option>)
                                            }
                                        </Form.Control>
                                        <span className={"hetu-form-error-span"}>
                                            {this.state.errors["connectorName"]}
                                        </span>
                                    </div>
                                </Col>
                            </Row>
                            <Row>
                                <Col style={{display: "flex", alignItems:'center'}}>
                                    <FormLabel className={"hetu-form-label"}>Catalog Name</FormLabel>
                                    <i className="fa fa-info-circle fa-form-label"
                                       title={"1.Name is case insensitive\n" +
                                       "2.Should be alphanumeric\n" +
                                       "3.Can contain special character underscore.\n" +
                                       "4.Max length: 100 characters"}></i>
                                </Col>
                                <Col>
                                    <div style={{display: "block"}}>
                                        <FormControl name="catalogName" type="text" maxLength={100} style={{flexDirection: "column"}} onChange={this.handleChange}
                                                     placeholder={"Catalog Name"} value={this.state.catalogName}/>
                                        <span className={"hetu-form-error-span"}>
                                            {this.state.errors["catalogName"]}
                                        </span>
                                    </div>
                                </Col>
                            </Row>
                            {(() => {
                                if (this.state.selectedConnector != "" && this.state.selectedConnector.connectorWithProperties.propertiesEnabled) {
                                    return (
                                        <div>
                                            <Row style={{display: "flex", alignItems:'center'}}>
                                                <Col style={{flexDirection: "column"}}>
                                                    <FormLabel className={"hetu-form-label"}>Catalog Properties</FormLabel>
                                                    <a href={this.state.selectedConnector.configLink} className="alert-link" target="_blank">
                                                        <i className="fa fa-info-circle fa-form-label"
                                                           title={"Click for more details of " + this.state.connectorName + "'s configurations details"}>
                                                        </i>
                                                    </a>
                                                </Col>
                                                <Col style={{flexDirection: "column"}}>
                                                    <Button className={"hetu-form-label-plus"} onClick={this.addProperty}>
                                                        <i className="material-icons" title={"Add property"}>
                                                        add</i>
                                                    </Button>
                                                </Col>
                                            </Row>
                                            {
                                                catalogProperties.map((key, idx) => {
                                                    let name = key["name"];
                                                    let value = key["value"];
                                                    let description = key ["description"];
                                                    let readOnly = key ["readOnly"] != undefined && key["readOnly"];
                                                    let rowKey = `row-${idx}`;
                                                    return (
                                                        <div key={idx}>
                                                            <Row style={{display: "flex",alignItems:'center'}} key={rowKey}>
                                                                <Col style={{flexDirection: "column", marginRight: '10px', width:"70%"}}>
                                                                    <FormControl name="name" type="text" data-id={idx} placeholder={"property name"}
                                                                                 value={name}
                                                                                 readOnly={readOnly}
                                                                                 onChange={this.handlePropertyChange.bind(this)}/>
                                                                </Col>
                                                                <Col style={{flexDirection: "column", width:"100%"}}>
                                                                    <FormControl name="value" type="text" data-id={idx} placeholder={"property value"}
                                                                                 value={value}
                                                                                 readOnly={readOnly}
                                                                                 onChange={this.handlePropertyChange.bind(this)}/>
                                                                </Col>
                                                                {(() => {
                                                                    if (description != undefined && description != "") {
                                                                        return (
                                                                            <Col style={{flexDirection: "column"}}>
                                                                                <i className="fa fa-info-circle fa-form-label"
                                                                                   title={description}>
                                                                                </i>
                                                                            </Col>);
                                                                    }
                                                                    else {
                                                                        return (<div style={{minWidth: "27px"}}></div>)
                                                                    }
                                                                })()}
                                                                <Col style={{flexDirection: "column"}}>
                                                                    <Button className={"hetu-form-label-minus"} data-id={idx}
                                                                            onClick={this.removeProperty.bind(this, idx)}>
                                                                        <i className="material-icons" title={"Remove property"}>
                                                                            remove</i>
                                                                    </Button>
                                                                </Col>
                                                            </Row>
                                                            {(this.state.errors.catalogProperties[idx] != "") &&
                                                            <span className={"hetu-form-error-span"}>{this.state.errors.catalogProperties[idx]}</span>
                                                            }
                                                        </div>
                                                    );
                                                })
                                            }
                                        </div>
                                    );
                                }
                                return null;
                            })()}
                            {(() => {
                                if (this.state.selectedConnector != "" && this.state.selectedConnector.connectorWithProperties.catalogConfigFilesEnabled) {
                                    return (
                                        <div>
                                            <Row style={{display: "flex",alignItems:'center'}}>
                                                <Col style={{flexDirection: "column"}}>
                                                    <FormLabel className={"hetu-form-label"}>Catalog Configuration Files</FormLabel>
                                                    <i className="fa fa-info-circle fa-form-label"
                                                       title={"Catalog configuration files are private to catalog"}>
                                                    </i>
                                                </Col>
                                                <Col style={{flexDirection: "column"}}>
                                                    <Button name="add-catalogconf" className={"hetu-form-label-plus"} onClick={this.addConfiguration}>
                                                        <i className="material-icons" title={"Add catalog file"}>
                                                            add</i>
                                                    </Button>
                                                </Col>
                                            </Row>
                                            {
                                                catalogConfigProperties.map((key, idx) => {
                                                    return (
                                                        <div key={idx}>
                                                            <Row style={{display: "flex",alignItems:'center'}} key={idx}>
                                                                <Col style={{flexDirection: "column"}}>
                                                                    <FormFileInput name="configfile" key="{idx}" data-id={idx}
                                                                                   onChange={this.handleConfigFileChange.bind(this)}/>
                                                                </Col>
                                                                <Col style={{flexDirection: "column"}}>
                                                                    <Button name="remove-conf"
                                                                            className={"hetu-form-label-minus"} data-id={idx}
                                                                            onClick={this.removeConfiguration.bind(this)}>
                                                                        <i className="material-icons" title={"Remove file"}>
                                                                            remove</i>
                                                                    </Button>
                                                                </Col>
                                                            </Row>
                                                            {this.state.errors.catalogConfigProperties[idx] != "" &&
                                                            <span className={"hetu-form-error-span"}>{this.state.errors.catalogConfigProperties[idx]}</span>
                                                            }
                                                        </div>
                                                    );
                                                })
                                            }
                                        </div>
                                    );
                                }
                                return null;
                            })()
                            }
                            {(() => {
                                if (this.state.selectedConnector != "" && this.state.selectedConnector.connectorWithProperties.globalConfigFilesEnabled) {
                                    return (
                                        <div>
                                            <Row style={{display: "flex", alignItems:'center'}}>
                                                <Col style={{flexDirection: "column"}}>
                                                    <FormLabel className={"hetu-form-label"}>Global Configuration Files</FormLabel>
                                                    <i className="fa fa-info-circle fa-form-label"
                                                       title={"Global configuration files are shared between other connectors and only need to upload once."}>
                                                    </i>
                                                </Col>
                                                <Col style={{flexDirection: "column"}}>
                                                    <Button name="add-globalconf" className={"hetu-form-label-plus"} onClick={this.addGlobalConfiguration.bind(this)}>
                                                        <i className="material-icons" title={"Add global file"}>
                                                            add</i>
                                                    </Button>
                                                </Col>
                                            </Row>
                                            {
                                                globalConfigProperties.map((key, idx) => {
                                                    return (
                                                        <div key={idx}>
                                                            <Row style={{display: "flex",alignItems:'center'}} key={idx}>
                                                                <Col style={{flexDirection: "column"}}>
                                                                    <FormFileInput name="global-configfile" key="{idx}" data-id={idx}
                                                                                   onChange={this.handleConfigFileChange.bind(this)}/>
                                                                </Col>
                                                                <Col style={{flexDirection: "column"}}>
                                                                    <Button name="remove-globalconf"
                                                                            className={"hetu-form-label-minus"} data-id={idx}
                                                                            onClick={this.removeGlobalConfiguration.bind(this)}>
                                                                        <i className="material-icons" title={"Remove file"}>
                                                                            remove</i>
                                                                    </Button>
                                                                </Col>
                                                                <Col style={{flexDirection: "column"}}>
                                                                </Col>
                                                            </Row>
                                                            {(this.state.errors.globalConfigProperties[idx] != "") &&
                                                            <span className={"hetu-form-error-span"}>{this.state.errors.globalConfigProperties[idx]}</span>
                                                            }
                                                        </div>
                                                    );
                                                })
                                            }
                                        </div>
                                    );
                                }
                                return null;
                            })()}
                            {/*<Row style={{display: "flex"}}>*/}
                            {/*    <Col style={{flexDirection: "column"}}>*/}
                            {/*        <Button onClick={this.handleSubmit} className={"btn btn-success"}>Submit</Button>*/}
                            {/*    </Col>*/}
                            {/*<Col style={{flexDirection: "column"}}>*/}
                            {/*    <Button onClick={this.handleReset.bind(this)}>Reset</Button>*/}
                            {/*</Col>*/}
                            {/*</Row>*/}
                        </FormGroup>
                    </Form>
                </div>
                <div className={'catalog-btn-part'}>
                    <Button onClick={this.handleSubmit} className={"btn btn-success btn-lg active"}>Submit</Button>
                    <Button onClick={this.handleClose} className={"btn btn-lg"}>Close</Button>
                </div>
            </div>
        );
    }
}

export default AddCatalog;