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
import UserActions from '../actions/UserActions';
import UserStore from '../stores/UserStore';

// State actions
function getStateFromStore() {
  return {
    user: UserStore.getCurrentUser()
  };
}

class Header
    extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      user: UserStore.getCurrentUser(),
      noConnection: false,
      lightShown: false,
      info: null,
      lastSuccess: Date.now(),
      modalShown: false,
      errorText: null,
    };
    this._onChange = this._onChange.bind(this);
  }

  componentDidMount() {
    UserStore.listen(this._onChange);
    UserActions.fetchCurrentUser();
    this.refreshLoop.bind(this)();
  }

  componentWillUnmount() {
    UserStore.unlisten(this._onChange);
  }

  refreshLoop() {
    clearTimeout(this.timeoutId);
    fetch("../v1/info")
    .then(response => response.json())
    .then(info => {
      this.setState({
        info: info,
        noConnection: false,
        lastSuccess: Date.now(),
        modalShown: false,
      });
    this.resetTimer();
    })
    .catch(error => {
      this.setState({
        noConnection: true,
        lightShown: !this.state.lightShown,
        errorText: error
      });
      this.resetTimer();
    });
  }

  resetTimer() {
    clearTimeout(this.timeoutId);
    this.timeoutId = setTimeout(this.refreshLoop.bind(this), 1000);
  }

  renderStatusLight() {
    if (this.state.noConnection) {
      if (this.state.lightShown) {
        return <span className="status-light status-light-red" id="status-indicator"/>;
      }
      else {
        return <span className="status-light" id="status-indicator"/>
      }
    }
    return <span className="status-light status-light-green" id="status-indicator"/>;
  }

  render() {
    const info = this.state.info;
    return (
      <header className='flex flex-row'>
        <div className='flex'>
          <a className={"hetu-header-brand-name"} href={"/"} style={{fontFamily:"roboto!important"}}>
            <img src={"assets/lk-logos.svg"} alt={"openLooKeng logo"} className={"hetu-header-brand-name"}/>
          </a>
        </div>
        <div className='flex justify-flex-end menu'>
          <div className='flex flex-initial version'>
            <div className="version-inner">
              Version :
              {info? info.nodeVersion.version : 'null'}
            </div>
            <div className="version-inner">
              Environment :
              {info ? info.environment : 'null'}
            </div>
            <div className="version-inner">
              Uptime
              : {info ? info.uptime : '0s'}
            </div>
            <div className="version-inner">
              <span data-toggle="tooltip" data-placement="bottom" title="Connection status">
                {this.renderStatusLight()}
              </span>
            </div>
          </div>
          <div className='flex flex-initial'>
            <div>
              <i className='glyphicon glyphicon-user'/>
              {this.state.user.name}
            </div>
            <div className="logout">
              <form method="post" action="../ui/api/logout">
                <button type="submit" className="btn btn-sm">
                  <i className='fa fa-sign-out'/>
                  Logout
                </button>
              </form>
            </div>
          </div>
          {/*<div className='flex flex-initial permissions'>*/}
          {/*  <i className='glyphicon glyphicon-lock' />*/}
          {/*  {this.state.user.executionPermissions.accessLevel}*/}
          {/*</div>*/}
        </div>
      </header>
    );
  }

  /* Store events */
  _onChange() {
    this.setState(getStateFromStore());
  }
}

export default Header;
