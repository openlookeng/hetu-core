/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import {getStatusText} from "./xhrutil"

const status = (response) => {
  if (response.status >= 200 && response.status < 300) {
    return Promise.resolve(response);
  } else {
    let message = "Error: " + getStatusText(response) + " (code: " + response.status + ");";
    let content = response.headers.get("Content-length");
    if (content != null && content > 0) {
      return response.text().then((msg) => {
        return Promise.reject(new Error(message + " " + msg));
      });
    }
    return Promise.reject(new Error(message));
  }
};

const json = (response) => {
  let content = response.headers.get("Content-length");
  if (response.status !== 204 && content != null && content > 0) {
    return response.json();
  } else {
    return {};
  }
};

const xhrform = (url, params = {}) => {
  params = Object.assign({
    credentials: 'same-origin'
  }, params)

  return fetch(url, params)
      .then(status)
      .then(json);
};

export default xhrform;
