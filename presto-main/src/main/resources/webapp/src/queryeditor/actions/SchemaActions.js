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
import xhr from "../utils/xhr";
import _ from "lodash"

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

    fetchSchemas(catalogs, refresh = false) {
        return xhr("../api/table/schemas?force=" + refresh).then((data) => {
            if (refresh) {
                catalogs = [];
            }
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
                            children: []
                        };
                        schemas.push(schema);
                    }
                }
                if (!refresh) {
                    //Remove deleted schemas
                    //Reverse iterate and mutate array by index.
                    let existingSchemas = catalog.children;
                    let schemaIndex = existingSchemas.length - 1;
                    while (schemaIndex >= 0) {
                        let currentSchema = existingSchemas[schemaIndex];
                        let fetchedSchema = entry.schemas.indexOf(currentSchema.name);
                        if (_.isUndefined(fetchedSchema) || fetchedSchema < 0) {
                            existingSchemas.splice(schemaIndex, 1);
                        }
                        schemaIndex -= 1;
                    }
                }
            }
            if (!refresh) {
                //Remove removed catalogs
                //Reverse iterate and mutate array by index.
                let index = catalogs.length - 1;
                while (index >= 0) {
                    let currentCatalog = catalogs[index];
                    let fetchedCatalog = _.find(data, {catalogName: currentCatalog.name});
                    if (_.isUndefined(fetchedCatalog)) {
                        catalogs.splice(index, 1);
                    }
                    index -= 1;
                }
            }
            return catalogs;
        }).then((catalogs) => {
            this.actions.updateSchemas(catalogs);
            return catalogs;
        });
    }

    fetchTables(catalogs) {
        return xhr("../api/table").then((data) => {
            for (const [index, entry] of data.entries()) {
                let catalog = _.find(catalogs, {name: entry.connectorId});
                if (_.isUndefined(catalog)) {
                    catalog = {
                        name: entry.connectorId,
                        type: dataType.CATALOG,
                        fqn: "schematree-catalog." + entry.catalogName,
                        children: []
                    };
                    catalogs.push(catalog);
                }
                let schemas = catalog.children;
                let schema = _.find(schemas, {name: entry.schema});
                if (_.isUndefined(schema)) {
                    schema = {
                        name: entry.schema,
                        type: dataType.SCHEMA,
                        catalog: entry.connectorId,
                        fqn: "schematree-schema." + catalog.name + "." + schemaName,
                        children: []
                    };
                    schemas.push(schema);
                }
                let tables = schema.children;
                let table = _.find(tables, {name: entry.table});
                if (_.isUndefined(table)) {
                    table = {
                        name: entry.table,
                        type: dataType.TABLE,
                        catalog: entry.connectorId,
                        schema: entry.schema,
                        fqn: entry.fqn
                    };
                    tables.push(table);
                }
            }
            catalogs.forEach(function (catalog, index, catalogsArray) {
                catalog.children.forEach(function (schema, schemaIndex, schemasArray) {
                    //Remove deleted tables
                    //Reverse iterate and mutate array by index.
                    let existingTables = schema.children;
                    let tableIndex = existingTables.length - 1;
                    while (tableIndex >= 0) {
                        let currentTable = existingTables[tableIndex];
                        let fetchedTable = _.find(data, {connectorId: catalog.name, schema: schema.name, table: currentTable.name});
                        if (_.isUndefined(fetchedTable)) {
                            existingTables.splice(tableIndex, 1);
                        }
                        tableIndex -= 1;
                    }
                })
            })
            return catalogs;
        }).then((catalogs) => {
            this.actions.updateTables(catalogs);
            return catalogs;
        });
    }
}

export default alt.createActions(SchemaActions);