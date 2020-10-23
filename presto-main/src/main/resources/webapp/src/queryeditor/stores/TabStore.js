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
import alt from '../alt';
import TabActions from '../actions/TabActions';
import TabConstants from '../constants/TabConstants';

class TabStore {
  constructor() {
    this.bindActions(TabActions);

    this.selectedTab = TabConstants.ALL_QUERIES;
    this.selectedLeftPanelTab = TabConstants.LEFT_PANEL_TREE;
  }

  onSelectTab(tab) {
    this.selectedTab = tab;
  }

  onSelectLeftPanelTab(tab) {
    this.selectedLeftPanelTab = tab;
  }

  static getSelectedTab() {
    return this.getState().selectedTab;
  }

  static getSelectedLeftPanelTab() {
    return this.getState().selectedLeftPanelTab;
  }
}

export default alt.createStore(TabStore, 'TabStore');
