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


class CollectionActions {
    constructor() {

    }

    addToCollection(queryText,catalog,schema) {
        return xhr("../v1/myCollection/collection", {
            method: 'post',
            headers :{
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: 'queryText=' + queryText + '&' + 'catalog=' + catalog + '&' + 'schema=' + schema
        }).then((data)=> {
            if(data==false||data=="false") {
                alert("Error! You have a same collection query.");
            }
        });
    }

    getCollection(queryText) {
        return xhr(`../v1/myCollection/collection/query?${queryText}`).then((data) => {
            return data;
        })
    }

    deleteCollection(queryText,catalog,schema) {
        return xhr(`../v1/myCollection/collection/delete`, {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            method: 'DELETE',
            body: 'queryText=' + queryText + '&' + 'catalog=' + catalog + '&' + 'schema=' + schema
        }).then((data)=> {
            if(data==true||data=="true") {
            }
            else{
                alert("delete failed!");
            }
        });
    }
}

export default alt.createActions(CollectionActions);
