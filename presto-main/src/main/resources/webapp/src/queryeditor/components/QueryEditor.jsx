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
import RunActions from '../actions/RunActions';
import AceEditor from 'react-ace';
import ResultsPreviewActions from '../actions/ResultsPreviewActions'
import QueryActions from '../actions/QueryActions'
import QueryStore from '../stores/QueryStore';
import _ from "lodash"

import 'brace/theme/eclipse';
import 'brace/mode/sql';
import 'brace/snippets/sql';
import 'brace/ext/language_tools';
import ModalDialog from "./ModalDialog";
import CnxnMonitorStore from "../stores/CnxnMonitorStore";
import CnxnMonitorActions from "../actions/CnxnMonitorActions";
import SchemaStore from "../stores/SchemaStore";
import {Form} from "react-bootstrap";

class QueryEditor
    extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      lastSubmissionResult: "",
      pollingError: "",
      errorDialogDetailsDisplay: "none",
      errorDialog: false,
      model: [],
      currentContext: {
        catalog: "system",
        schema: "runtime"
      },
      buttonState:false
    }
    ;
    this.query = "select * from system.information_schema.tables;";
    this.queryEditorRef = React.createRef();
    this.queryContainerRef = React.createRef();
    this.contextCatalogRef = React.createRef();
    this.contextSchemaRef = React.createRef();
    this.handleRun = this.handleRun.bind(this);
    this.onChange = this.onChange.bind(this);
    this._selectQuery = this._selectQuery.bind(this);
    this.errorHandler = this.errorHandler.bind(this);
    this.schemaContextHandler = this.schemaContextHandler.bind(this);
    this.renderErrorDialogFooter = this.renderErrorDialogFooter.bind(this);
    this.renderErrorInfo = this.renderErrorInfo.bind(this);
    this.renderMenubar = this.renderMenubar.bind(this);
    this.closeErrorDialog = this.closeErrorDialog.bind(this);
    this.errorCollapseToggle = this.errorCollapseToggle.bind(this);
    this.getBriefErrorMessage = this.getBriefErrorMessage.bind(this);
  }

  componentDidMount() {
    QueryStore.listen(this._selectQuery);
    CnxnMonitorStore.listen(this.errorHandler);
    SchemaStore.listen(this.schemaContextHandler);
  }

  componentWillUnmount() {
    QueryStore.unlisten(this._selectQuery);
    CnxnMonitorStore.unlisten(this.errorHandler);
    SchemaStore.unlisten(this.schemaContextHandler);
  }

  closeErrorDialog() {
    CnxnMonitorActions.clear();
    var state = this.state;
    Object.assign(state, {
      lastSubmissionResult: "",
      lastPollingError: "",
      errorDialogDetailsDisplay: "none",
      errorDialog: false
    })
    this.setState(state)
  }

  errorCollapseToggle() {
    let state = this.state;
    state.errorDialogDetailsDisplay = (state.errorDialogDetailsDisplay == "none") ? "block" : "none";
    this.setState(state);
  }

  getBriefErrorMessage() {
    let message = "" + (this.state.lastSubmissionResult == "" ?
        this.state.lastPollingError : this.state.lastSubmissionResult);
    let delim = message.indexOf(";");
    if (delim > -1) {
      return message.substr(0, delim)
    }
    else {
      return message;
    }
  }

  renderErrorInfo() {
    if (this.state.lastSubmissionResult === '' && this.state.lastPollingError === '') {
      return null;
    } else {
      let message;
      let header;
      if (this.state.lastSubmissionResult != "") {
        message = this.state.lastSubmissionResult;
        header = "Query submission Error";
      }
      else {
        message = this.state.lastPollingError;
        header = "Polling Error"
      }
      var detailsHidden = this.state.errorDialogDetailsDisplay == "none";
      return (
          <ModalDialog show={this.state.errorDialog}
                       header={header}
                       footer={this.renderErrorDialogFooter()}
                       onClose={this.closeErrorDialog}>
            <div style={{color: '#ff0000',padding:'10px 20px'}}>
              <h4 style={{alignSelf: "center"}}>
                {this.getBriefErrorMessage()}
              </h4>
              <button className={"hetu-error-dialog-collapsible " + (detailsHidden ? "" : "hetu-error-dialog-collapsible-active")}
                      onClick={this.errorCollapseToggle}>Details:</button>
              <div className={"hetu-error-dialog-collapsible-content"} style={{display: this.state.errorDialogDetailsDisplay}}>
                <pre>{message}</pre>
              </div>
            </div>
          </ModalDialog>
      );
    }
  }

  renderErrorDialogFooter() {
    return (
        <div className={"flex flex-row justify-flex-end btn-toolbar btn-group net-error-dialog"}>
          <button className={"btn btn-error"}>
            <a href={"/"}>Reload</a>
          </button>
          <button className={"btn"}>
            <a href={"#"} onClick={this.closeErrorDialog} style={{color:'#666'}}>Continue</a>
          </button>
        </div>
    );
  }

  handleContextChange(e) {
    e.preventDefault();
    let newCatalog;
    let newSchema;
    let state;
    if (e.target.name == "catalog") {
      newCatalog = e.target.value
      if (newCatalog == "") {
        return;
      }
      let schemaModel = SchemaStore.getModel();
      let newCatalogModel = _.find(schemaModel, {name: newCatalog})
      if (_.isUndefined(newCatalogModel)) {
        return;
      }
      newSchema = newCatalogModel.children.length > 0 ? newCatalogModel.children[0].name : "";
      state = this.state;
      state.currentContext = {
        catalog: newCatalog,
        schema: newSchema
      }
      this.setState(state);
    }
    else if (e.target.name == "schema") {
      newSchema = e.target.value;
      let newCatalogModel = _.find(this.state.model, {name: this.state.currentContext.catalog})
      if (_.isUndefined(newCatalogModel)) {
        newSchema = "";
      }
      else if (_.isUndefined(_.find(newCatalogModel.children, {name: newSchema}))) {
        newSchema = newCatalogModel.children.length > 0 ? newCatalogModel.children[0].name : "";
      }
      state = this.state;
      state.currentContext.schema = newSchema;
      this.setState(state);
    }
  }

  renderMenubar() {
    let currentCatalog = this.state.currentContext.catalog;
    let currentSchema = this.state.currentContext.schema;
    let catalogs = this.state.model;
    let currentCatalogModel = _.find(catalogs, {name: currentCatalog})
    let schemas = (_.isUndefined(currentCatalogModel)) ? [] : currentCatalogModel.children;
    return (
        <div style={{display:"flex"}}>
          <button className="btn btn-success btn-sm runBtn active"
                  style={{marginRight:"35px", marginLeft:"35px"}}
                  disabled={this.state.buttonState}
                  onClick={this.handleRun}
          title={"Submit query"}>Run
          </button>
          <Form.Label style={{marginTop:"10px", maxHeight:"30px", textAlign:"end"}}>Catalog:</Form.Label>
          <Form.Control as="select" name="catalog" value={currentCatalog} onChange={this.handleContextChange.bind(this)}
                        style={{fontsize:"15px", margin: "5px", width:"100px"}}>
            {
              catalogs.map((key) => {
                if (key.children.length == 0) {
                  //Dont include catalogs without schemas
                  return null
                }
                return <option key={key.name} value={key.name}>{key.name}</option>
              })
            }
          </Form.Control>
          <Form.Label style={{position: "relative", top: "10px", maxHeight:"30px", textAlign:"end"}}>Schema:</Form.Label>
          <Form.Control as="select" name="schema" value={currentSchema} onChange={this.handleContextChange.bind(this)}
                        style={{fontsize:"15px", margin: "5px",  width:"100px"}}>
            {
              schemas.map((key) => {
                return <option key={key.name} value={key.name}>{key.name}</option>
              })
            }
          </Form.Control>
        </div>
    )
  }

  render() {
    return (
      <div className="flex flex-initial flex-column">
        <div className="flex flex-row editor-menu">
          <div className='flex '>
            {this.renderMenubar()}
            {this.renderErrorInfo()}
          </div>
          <div className='flex justify-flex-end'>
          </div>
        </div >
        <div ref={this.queryContainerRef} className="editor-container clearfix">
          <div className="editor">
            <AceEditor
                ref={this.queryEditorRef}
                mode="sql"
                theme={"eclipse"}
                onChange={this.onChange}
                name="editor"
                highlightActiveLine={true}
                editorProps={{
                }}
                width={"100%"}
                height={"100%"}
                defaultValue={this.query}
                fontSize={16}
                wrapEnabled={true}
                setOptions={{
                  enableBasicAutocompletion: true,
                  enableLiveAutocompletion: true,
                  enableSnippets: true,
                  showLineNumbers: true,
                  tabSize: 2
                }}
            />
          </div>
        </div>
      </div>);
  }

  // - Internal events ----------------------------------------------------- //
  handleRun() {
    let query = this._getQuery();
    if (query == "" || query.trim() == "") {
      return;
    }
    let state = this.state;
    state.buttonState = !this.state.buttonState
    this.setState(state);
    ResultsPreviewActions.clearResultsPreview();
    let sessionContext = this.state.currentContext;
    QueryActions.selectQuery({query, sessionContext});
    RunActions.execute({
      query: query,
      sessionContext: sessionContext
    });
    setTimeout(()=>{
      let state = this.state;
      state.buttonState = !this.state.buttonState
      this.setState(state);
    },1000)
  }

  // - Internal helpers ---------------------------------------------------- //

  // Retrieves the current query
  // @return {string} the query string
  _getQuery() {
    return this.query;
  }

  // Checks or there is currently something selected
  // @return {boolean} is the start equal to the end of the selection
  _isRangeStartSameAsEnd(range) {
    let start = range.start, end   = range.end;

    // Return of the start equals the end of the selection
    return !!start && !!end &&
      (start.row === end.row) &&
      (start.column === end.column);
  }

  // Populate the editor with a given query.
  _selectQuery() {
    let selectedQuery = QueryStore.getSelectedQuery();
    if (selectedQuery != null) {
      this.queryEditorRef.current.editor.setValue(selectedQuery);
      this.query = selectedQuery;
    }
    let context = QueryStore.getSessionContext();
    if (context != null) {
      let state = this.state;
      state.currentContext.catalog = context.catalog
      state.currentContext.schema = context.schema
      this.setState(state);
    }
    this.queryEditorRef.current.editor.focus()
    this.queryEditorRef.current.editor.clearSelection()
  }

  errorHandler() {
    if (this.state.errorDialog) {
      //If error dialog is already opened, we can ignore further notices until action is taken.
      return;
    }
    let state = this.state;
    state.lastSubmissionResult = CnxnMonitorStore.getLastSubmissionResult();
    state.lastPollingError = CnxnMonitorStore.getLastPollingError();
    state.errorDialog = state.lastSubmissionResult != "" || state.lastPollingError != ""
    this.setState(state);
  }

  onChange(newValue) {
    this.query = newValue;
  }

  schemaContextHandler() {
    let model = SchemaStore.getModel();
    let state = this.state;
    state.model = model;
    this.setState(state);
  }
}

export default QueryEditor;
