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
import RunActions from '../actions/RunActions';
import UserApiUtils from '../utils/UserApiUtils';
import RunApiUtils from '../utils/RunApiUtils';
import logError from '../utils/logError'

class UserActions {
  fetchCurrentUser() {
    UserApiUtils.fetchCurrentUser().then((user) => {
      this.actions.receivedCurrentUser(user);
    //
    //   // Now fetch queries for that user.
    //   return RunApiUtils.fetchForUser(user);
    // }).then((results) => {
    //   RunActions.addMultipleRuns(results);
    }).catch(logError);
  }

  receivedCurrentUser(user) {
    this.dispatch(user);
  }
}

module.exports = alt.createActions(UserActions);
