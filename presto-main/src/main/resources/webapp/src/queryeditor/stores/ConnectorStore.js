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
import alt from '../alt'
import FluxCollection from '../utils/FluxCollection'
import ConnectorActions from '../actions/ConnectorActions'

class ConnectorStore {
  constructor() {
    // handle store listeners
    this.bindListeners({
      onReceivedConnectors: ConnectorActions.RECEIVED_CONNECTORS,
      onReceivedConnector: ConnectorActions.RECEIVED_CONNECTOR,
      onReceivedOriginalConnectors: ConnectorActions.RECEIVED_ORIGINAL_CONNECTORS
    });

    // export methods we can use
    this.exportPublicMethods({
      getCollection: this.getCollection,
    });

    // state
    this.collection = new FluxCollection({
      comparator: (model, index) => -1 * index
    });
  }

  onReceivedConnector(connector) {
    this.collection.add(connector);
  }

  onReceivedConnectors(connectors) {
    this.collection.add(connectors);
  }

  onReceivedOriginalConnectors(connectors) {
    this.collection.add(connectors,{update:true});
  }

  getCollection() {
    return this.getState().collection;
  }
}

export default alt.createStore(ConnectorStore, 'ConnectorStore');
