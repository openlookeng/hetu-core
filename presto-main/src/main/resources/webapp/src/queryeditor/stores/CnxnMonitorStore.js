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
import alt from '../alt';
import CnxnMonitorActions from "../actions/CnxnMonitorActions";

class CnxnMonitorStore {
    constructor() {
        this.bindListeners({
            onSubmitSuccess: CnxnMonitorActions.SUBMIT_SUCCESS,
            onSubmitFailure: CnxnMonitorActions.SUBMIT_FAILED,
            onPollingFailure: CnxnMonitorActions.POLLING_FAILED,
            onClear: CnxnMonitorActions.CLEAR
        });
        this.lastSubmittedQuery = "";
        this.lastSubmissionResult = "";
        this.pollingFaiureCount = 0;
        this.lastPollingError = "";
        this.exportPublicMethods({
            getLastSubmissionResult: this.getLastSubmissionResult,
            getLastSubmittedQuery: this.getLastSubmittedQuery,
            getLastPollingError: this.getLastPollingError,
        });
    }

    onSubmitSuccess(query) {
        this.lastSubmittedQuery = query;
        this.lastSubmissionResult = "";
    }

    onSubmitFailure(errorInfo) {
        this.lastSubmittedQuery = errorInfo.query;
        this.lastSubmissionResult = errorInfo.error;
    }

    onPollingFailure(errorInfo) {
        this.pollingFaiureCount++;
        if (this.pollingFaiureCount < 5) {
            //dont raise the concern until 5 attempts = about 15 seconds when runningQueries present,
            // otherwise about 3min 10 seconds and next every to 5 minutes (if continued).
            return;
        }
        this.lastPollingError = errorInfo.error;
    }

    getLastSubmissionResult() {
        return this.getState().lastSubmissionResult;
    }

    getLastSubmittedQuery() {
        return this.getState().lastSubmittedQuery;
    }

    getLastPollingError() {
        return this.getState().lastPollingError;
    }

    onClear() {
        this.pollingFaiureCount = 0;
        this.lastSubmissionResult = "";
        this.lastSubmittedQuery = "";
        this.lastPollingError = "";
    }
}

export default alt.createStore(CnxnMonitorStore, 'CnxnMonitorStore');
