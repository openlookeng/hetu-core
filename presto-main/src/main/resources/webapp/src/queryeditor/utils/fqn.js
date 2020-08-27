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
import _ from 'lodash';

function validFqnPart(part) {
  return _.isString(part) && !_.isEmpty(part);
}

function getParts(fqn) {
  if (!(!!fqn && _.isString(fqn))) {
    return null;
  }
  return fqn.split('.');
}

function schema(fqn) {
  let parts = getParts(fqn);

  if (parts && parts.length) {
    let schemaParts = parts[0];
    let i = 1;
    for (;i < parts.length - 1;i++) {
      schemaParts = schemaParts + '.' + parts[i];
    }
    return schemaParts;
  } else {
    return null;
  }
}

function catalog(fqn) {
  let parts = getParts(fqn);

  if (parts && parts.length) {
    let catalogParts = parts[0];
    let i = 1;
    for (; i < parts.length - 2; i++) {
      catalogParts = catalogParts + '.' + parts[i];
    }
    return catalogParts;
  }
  else {
    return null;
  }
}

function simpleSchema(fqn) {
  let parts = getParts(fqn);

  if (parts && parts.length >= 2) {
    return parts[parts.length - 2];
  }
  else {
    return null;
  }
}

function table(fqn) {
  let parts = getParts(fqn);

  if (parts && parts.length) {
    return parts[parts.length - 1];
  } else {
    return null;
  }
}

function toFqn(schema, table) {
  return [schema, table].join('.');
}

export default {
  validFqnPart,
  schema,
  table,
  toFqn,
  catalog,
  simpleSchema
};
