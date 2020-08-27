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
import RunApiUtils from '../utils/RunApiUtils';
import logError from '../utils/logError'
import CnxnMonitorActions from "./CnxnMonitorActions";

class RunActions {
  constructor() {
    this.generateActions(
      'addMultipleRuns',
      'addRun',
      'connect',
      'disconnect',
      'handleConnectionError',
      'handleConnectionOpen',
      'resetOnlineStatus',
      'wentOffline',
      'wentOnline'
    );
  }

  fetchHistory() {
    return RunApiUtils.fetchHistory().then((results) => {
      this.actions.addMultipleRuns(results);
      return true;
    }).catch((error) => {
      logError(error);
      CnxnMonitorActions.pollingFailed({
        error: error.message
      });
      return false;
    })
  }

  execute(query) {
    RunApiUtils.execute(query).then((runObject) => {
      this.dispatch();
      this.actions.addRun(runObject);
      CnxnMonitorActions.submitSuccess(query.query);
    }).catch((error) => {
      logError(error);
      CnxnMonitorActions.submitFailed({
        query: query.query,
        error: error.message
      });
    });
  }

  kill(uuid) {
    RunApiUtils.kill(uuid);
  }

  handleConnectionMessage(data) {
    this.dispatch(data.job);
  }
}

export default alt.createActions(RunActions);
