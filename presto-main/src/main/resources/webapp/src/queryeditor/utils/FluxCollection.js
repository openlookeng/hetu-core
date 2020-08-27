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

class FluxCollection {
  constructor(opts = {}) {
    Object.assign(this, {
      collection: [],
      comparator: 'uuid'
    }, opts);
  }

  /* - Selectors ----------------------------------------------------------- */

  // Returns all items in the collection with an optional comparator
  // if no comparator is given, the default one is used
  // @param options {object} the options for this function
  // @return {array} the collection
  all(options = {}) {
    if (this.size() === 0) return [];

    // Return the sorted collection
    if (options.sort) {
      return _.sortBy(this.collection, options.sortBy || this.comparator)
    }
    return this.collection;
  }

  // Get a specific query from the collection
  // @param {integer} the event uuid
  // @return {object/undefined} the query object
  get(uuid) {
    return _.find(this.collection, { uuid: uuid });
  }

  // Filters the collection based on the arguments
  // @param context {Object/Function/Array} the search context
  where(context, options = {}) {
    if (_.isEmpty(context) || this.size() === 0) return [];

    // Filter the collection
    let results = _.where(this.collection, context);

    // Sort the result, if asked
    if (options.sort) {
      return _.sortBy(results, options.sortBy || this.comparator);
    }
    return results;
  }

  // Defines the size of the store collection
  // @return {Integer} the size of the store collection
  size() {
    return _.size(this.collection);
  }

  /* - Modifiers ----------------------------------------------------------- */

  // Simply creates a new object and adds it to the collection. Make absolutely
  // sure we don't got the object
  // @param models {Object} the model object
  // @return {Object} the store
  add(models, options = {update: false}) {
    // Convert a single object to an array
    let singular = !_.isArray(models);
    models = singular ? (models ? [models] : []) : _.clone(models)

    // Loop over the array and try to add them to the collection.
    _.each(models, (model) => {
      // Make sure this is a unique item. If there is already a match, fail this.
      let unique = _.find(this.collection, { uuid: model.uuid });
      if (!_.isUndefined(unique)) {
        if (options.update) {
          this.update(model.uuid, model);
        }
        return;
      }

      // Add the model to the collection
      this.collection.push(model);
    });

    // Return this for chaining purpose
    return this;
  }

  // Updates a model based on the uuid and the new object data
  // @param uuid {String} the uuid of the model
  // @param changedObject {Object} the updated run
  // @return {Object} the store
  update(uuid, changedObject, options = {}) {
    // Find the correct entry and update it with the new info
    let model = _.find(this.collection, { uuid: uuid }) || {};

    // Apply all the data to the object
    model = _.assign(model, changedObject);

    // Remove the old object and add the new one
    this.collection = _.reject(this.collection, { uuid: uuid });
    this.collection.push(model);

    return this;
  }

  // Removes a model from the collection
  // @param uuid {String} the uuid of the model
  // @param options {?Object}
  // @return {Object} the store
  remove(uuid, options = {}) {
    // Find the correct entry and update it with the new info
    let index = _.findIndex(this.collection, { uuid: uuid });

    if (index !== -1) {
      // Remove the old object
      this.collection.splice(index, 1);
    }

    return this;
  }

  clear() {
    this.collection = [];
  }

  contains(uuid) {
    let unique = _.find(this.collection, {uuid: uuid});
    if (_.isUndefined(unique)) {
      return false;
    }
    return true;
  }
}

export default FluxCollection;
