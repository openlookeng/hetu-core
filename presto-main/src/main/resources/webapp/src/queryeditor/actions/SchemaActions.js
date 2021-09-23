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
import alt from '../alt';
import xhr from "../utils/xhr";
import _ from "lodash";
import xhrform from "../utils/xhrform";
import UserStore from "../stores/UserStore";

export const dataType = {
    ROOT: "ROOT",
    CATALOG: "CATALOG",
    SCHEMA: "SCHEMA",
    TABLE: "TABLE"
}

class SchemaActions {
    constructor() {
        this.generateActions(
            'updateSchemas',
            'updateTables',
        );
    }

    deleteCatalog(catalogName) {
        return xhrform(`../v1/catalog/${catalogName}`, {
            headers: {
                "X-Presto-User": UserStore.getCurrentUser().name,
                "Accept": "application/json"
            },
            method: 'DELETE'
        }).then(() => {
            return {
                result: true,
                message: "Success"
            }
        }).catch((error) => {
            return {
                result: false,
                message: error.message
            }
        })
    }

    fetchSchemas(catalogs) {
        return xhr("../api/metadata/schemas").then((data) => {
            catalogs = [];
            for (const [index, entry] of data.entries()) {
                let catalog = _.find(catalogs, {name: entry.catalogName});
                if (_.isUndefined(catalog)) {
                    catalog = {
                        name: entry.catalogName,
                        type: dataType.CATALOG,
                        fqn: "schematree-catalog." + entry.catalogName,
                        children: []
                    };
                    catalogs.push(catalog);
                }
                for (const [index2, schemaName] of entry.schemas.entries()) {
                    let schemas = catalog.children;
                    let schema = _.find(schemas, {name: schemaName});
                    if (_.isUndefined(schema)) {
                        schema = {
                            name: schemaName,
                            type: dataType.SCHEMA,
                            catalog: catalog.name,
                            fqn: "schematree-schema." + catalog.name + "." + schemaName,
                            children: () => {
                                return new Promise((resolve, reject) => {
                                    var schemaTables = xhr("../api/metadata/tables/" + catalog.name + "/" + schemaName).then((data) => {
                                       var tables = [];
                                       for (const [index, entry] of data.entries()) {
                                           var table = {
                                               name: entry.table,
                                               type: dataType.TABLE,
                                               catalog: entry.connectorId,
                                               schema: entry.schema,
                                               fqn: entry.fqn
                                           };
                                           tables.push(table);
                                       }
                                       return tables;
                                    });
                                    // resolves into the children model
                                    resolve(schemaTables);
                                });
                            }
                        };
                        schemas.push(schema);
                    }
                }
            }
            return catalogs;
        }).then((catalogs) => {
            this.actions.updateSchemas(catalogs);
            return catalogs;
        });
    }

    fetchChildren(catalogs, item) {
        if (item.type == dataType.CATALOG) {
            return xhr("../api/metadata/schemas/" + item.name).then((data) => {
                let catalog = _.find(catalogs, {name: item.name});
                if (!_.isUndefined(catalog)) {
                    let schemas = [];
                    for (const [index, schemaName] of data.schemas.entries()) {
                        let schema = {
                            name: schemaName,
                            type: dataType.SCHEMA,
                            catalog: catalog.name,
                            fqn: "schematree-schema." + catalog.name + "." + schemaName,
                            children: () => {
                                return new Promise((resolve, reject) => {
                                    var schemaTables = xhr("../api/metadata/tables/" + catalog.name + "/" + schemaName).then((data) => {
                                       var tables = [];
                                       for (const [index, entry] of data.entries()) {
                                           var table = {
                                               name: entry.table,
                                               type: dataType.TABLE,
                                               catalog: entry.connectorId,
                                               schema: entry.schema,
                                               fqn: entry.fqn
                                           };
                                           tables.push(table);
                                       }
                                       return tables;
                                    });
                                    // resolves into the children model
                                    resolve(schemaTables);
                                });
                            }
                        };
                        schemas.push(schema);
                    }
                    catalog.children = schemas;
                }
                return catalogs;
            }).then((catalogs) => {
                this.actions.updateSchemas(catalogs);
                return catalogs;
            });
        }
        else if (item.type == dataType.SCHEMA) {
            return xhr("../api/metadata/tables/" + item.catalog + "/" + item.name).then((data) => {
                let catalog = _.find(catalogs, {name: item.catalog});
                if (!_.isUndefined(catalog))  {
                    let schema = _.find(catalog.children, {name: item.name});
                    if (!_.isUndefined(schema)) {
                        var tables = [];
                        for (const [index, entry] of data.entries()) {
                            var table = {
                                name: entry.table,
                                type: dataType.TABLE,
                                catalog: entry.connectorId,
                                schema: entry.schema,
                                fqn: entry.fqn
                            };
                            tables.push(table);
                        }
                        schema.children = tables;
                    }
                }
                catalog.children = [...catalog.children];
                return catalogs;
            }).then((catalogs) => {
                this.actions.updateTables(catalogs);
                return catalogs;
            });
        }
    }
}

export default alt.createActions(SchemaActions);