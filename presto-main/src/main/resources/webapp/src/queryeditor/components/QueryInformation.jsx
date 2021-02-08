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
import TabActions from '../actions/TabActions';

import ResultsTable from './ResultsTable';
import TableStore from '../stores/TableStore';
import TabStore from '../stores/TabStore';
import AllRunningQueries from './AllRunningQueries';
import DataPreview from './DataPreview';

import TabConstants from '../constants/TabConstants';

import {Tab, TabList, TabPanel, Tabs} from 'react-tabs';
import MySavedQueries from "./MySavedQueries";

class QueryInformation
    extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tableWidth: 400,
      tableHeight: 400,
      dataPreview: TableStore.getActiveTable(),
      selectedTab: TabStore.getSelectedTab()
    };
    this.QueryInformationRef = React.createRef();
    this.update = this.update.bind(this);
    this.onChange = this.onChange.bind(this);
    this.onResize = this.onResize.bind(this);
    this.onTabChange = this.onTabChange.bind(this);
    this.onTabSelect = this.onTabSelect.bind(this);
  }

  componentDidMount() {
    RunActions.connect();
    TableStore.listen(this.onChange);
    TabStore.listen(this.onTabChange);

    this.update();
    let win = window;

    if (win.addEventListener) {
      win.addEventListener('resize', this.onResize, false);
    } else if (win.attachEvent) {
      win.attachEvent('onresize', this.onResize);
    } else {
      win.onresize = this.onResize;
    }

    $(window).on('resize', this.update);
  }

  componentWillUnmount() {
    RunActions.disconnect();
    TableStore.unlisten(this.onChange);
    TabStore.unlisten(this.onTabChange);
  }

  update() {
    let windowHeight = document.body.clientHeight;
    let windowWidth = document.documentElement.clientWidth;
    let newWidth = windowWidth - 75 - 270 - 10; //left side size
    newWidth = 400 > newWidth ? 400 : newWidth;
    let newHeight = windowHeight - ((0.3 * (windowHeight + 50)) + 40 + 57 + 46 + 6); // editor + header + tabs + footer+ extra 6(unknown) heights
    newHeight = 400 > newHeight ? 400 : newHeight;
    this.setState({
      tableWidth: newWidth,
      tableHeight: newHeight,
    });
  }

  onChange() {
    const table = TableStore.getActiveTable();
    if (!table) return;
    if (this.state.dataPreview && table.name === this.state.dataPreview.name) return;

    this.setState({
      dataPreview: table,
    });

    // TabActions.selectTab.defer(TabConstants.DATA_PREVIEW);
  }

  onTabChange() {
    const selectedTab = TabStore.getSelectedTab();

    this.setState({selectedTab});
  }

  onResize() {
    this.update();
  }

  render() {
    return (
      <div ref={this.QueryInformationRef} className="flex flex-column query-information">
        <Tabs className='flex' onSelect={this.onTabSelect} selectedIndex={this.state.selectedTab}>
          <TabList>
            <Tab>Sample Queries</Tab>
            <Tab>All Queries</Tab>
            <Tab>Results</Tab>
            <Tab>Data Preview</Tab>
          </TabList>
          <TabPanel style={{height:"calc(70vh - 200px)", overflowY:'auto'}}>
            <MySavedQueries/>
          </TabPanel>
          <TabPanel style={{overflowY:'auto',height:"calc(70vh - 200px)"}}>
            <AllRunningQueries
                tableWidth={this.state.tableWidth}
                tableHeight={this.state.tableHeight}/>
          </TabPanel>
          <TabPanel style={{overflowY:'auto',height:"calc(70vh - 200px)"}}>
            <ResultsTable
                tableWidth={this.state.tableWidth}
                tableHeight={this.state.tableHeight}/>
          </TabPanel>
          <TabPanel style={{overflowY:'auto',height:"calc(70vh - 200px)"}}>
            <DataPreview
                tableWidth={this.state.tableWidth}
                tableHeight={this.state.tableHeight}/>
          </TabPanel>
        </Tabs>
      </div>);
  }

  onTabSelect(selectedTab) {
    TabActions.selectTab(selectedTab);
  }
}

export default QueryInformation;
