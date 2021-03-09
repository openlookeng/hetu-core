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
// import ConnectionErrors from './ConnectionErrors';
import Header from './Header';
import StatusFooter from "./StatusFooter";
import Footer from "./Footer";
// import TableExplorer from './TableExplorer';
import QueryInformation from './QueryInformation';
import QueryEditor from './QueryEditor';
import LeftPanel from "./LeftPanel";
import NavigationMenu from "../../NavigationMenu";

class HetuQueryEditorApp
    extends React.Component {
  componentDidMount() {
    // Add event listeners to the window to detect online/offline changes
    // for the user
    window.addEventListener('online',   function() { RunActions.wentOnline(); });
    window.addEventListener('offline',  function() { RunActions.wentOffline(); });
  }

  render() {
    return (
      <div className='hetu-app flex-column'>
        <div className='flex flex-row flex-initial header' style={{height: "61px"}}>
          <Header />
        </div>
        <div className='flex flex-row content'>
            <NavigationMenu active={"queryeditor"}/>
          <div className='flex flex-column flex-initial left' style={{height: "100%"}}>
              <div className='flex flex-column content' style={{height: "100%"}}>
                  <LeftPanel />
                  {/*<TableSearch />*/}
                  {/*<ColumnsPreview />*/}
             </div>
          </div>
          <div className='flex flex-column right' style={{height: "100%"}}>
              <div className='flex flex-column' style={{height: "40%"}}>
                  <QueryEditor />
              </div>
              <div className='flex flex-column' style={{height: "60%"}}>
                  <QueryInformation />
              </div>
          </div>
        </div>
        <div className='flex flex-row flex-initial statusFooter'>
            <StatusFooter/>
        </div>
        <div className='flex flex-row flex-initial footer'>
            <Footer/>
        </div>
      </div>);
  }
}

export default HetuQueryEditorApp;
