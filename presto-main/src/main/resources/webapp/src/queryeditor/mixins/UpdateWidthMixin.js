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
'use strict';

import _ from "lodash";

/**
 * Mixin to update the `width` state based on the DOM element's width whenever
 * the window is resized.
 */

export default {
  getInitialState() {
    return {
      width: 960
    };
  },

  componentDidMount() {
    /**
     * Create a unique resize handler for each component; otherwise the
     * debounce prevents this from working with multiple components.
     */
    this._onResize = _.debounce((e => this.recomputeWidth()), 50);

    $(window).on('resize', this._onResize);

    /**
     * Wait a few ms to make sure this happens in time.
     */
    _.defer((() => this.recomputeWidth()), 10);
  },

  componentWillUnmount() {
    $(window).off('resize', this._onResize);
  },

  recomputeWidth() {
    let width = $(this.getDOMNode()).innerWidth();
    this.setState({
      width
    });
  }
};
