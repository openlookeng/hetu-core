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

import xhr from "../utils/xhr";
import alt from "../alt";

class AuditLogActions {
    constructor() {
    }

    getAuditLog(user , beginTime , endTime , level , type) {
        return xhr(`../v1/audit/${type}`, {
            method: 'post',
            headers :{
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: 'user=' + user + '&' + 'beginTime=' + beginTime + '&' + 'endTime=' + endTime + '&' + 'level=' + level + '&' + 'type=' + type
        }).then((data)=> {
            return data ;
        });
    }

    downloadAuditLog(user , beginTime , endTime , level , type) {
        return xhr(`../v1/audit/download`, {
            method: 'post',
            headers :{
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: 'user=' + user + '&' + 'beginTime=' + beginTime + '&' + 'endTime=' + endTime + '&' + 'level=' + level + '&' + 'type=' + type
        });
    }
}

export default alt.createActions(AuditLogActions);
