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
import UserActions from '../actions/UserActions';

class UserStore {
  constructor() {
    this.bindListeners({
      onReceivedCurrentUser: UserActions.RECEIVED_CURRENT_USER
    });

    this.exportPublicMethods({
      getDefaultUser: this.getDefaultUser,
      getCurrentUser: this.getCurrentUser
    });

    this.user = this.getDefaultUser();
  }

  onReceivedCurrentUser(user) {
    this.user = user;
  }

  getDefaultUser() {
    return {
      name: 'lk',
      executionPermissions: {
        accessLevel: 'default',
        canCreateCsv: false,
        canCreateTable: false
      }
    };
  }

  getCurrentUser() {
    return this.getState().user;
  }
}

export default alt.createStore(UserStore, 'UserStore');
