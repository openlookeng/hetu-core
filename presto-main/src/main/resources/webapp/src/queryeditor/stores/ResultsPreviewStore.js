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
import alt from '../alt'
import _ from 'lodash';
import FluxCollection from '../utils/FluxCollection'
import ResultsPreviewActions from '../actions/ResultsPreviewActions'

class ResultsPreviewStore {
  constructor() {
    // handle store listeners
    this.bindListeners({
      onLoadResultsPreview: ResultsPreviewActions.RECEIVED_RESULTS_PREVIEW,
      onClearPreview: ResultsPreviewActions.CLEAR_RESULTS_PREVIEW,
      onSelectPreviewQuery: ResultsPreviewActions.SELECT_PREVIEW_QUERY,
      onSetTableColumnWidth: ResultsPreviewActions.SET_TABLE_COLUMN_WIDTH,
    });

    // export methods we can use
    this.exportPublicMethods({
      getPreviewQuery: this.getPreviewQuery,
      getResultsPreview: this.getResultsPreview,
    });

    // state
    this.preview = null;
    this.previewQuery = null;
  }

  onClearPreview() {
    this.preview = null;
  }

  onSelectPreviewQuery(query) {
    this.previewQuery = query;
  }

  onLoadResultsPreview(preview) {
    this.preview = _.extend(preview, {
      columnWidths: preview.columns.map(() => 120),
    });
  }

  onSetTableColumnWidth({ columnIdx, width }) {
    if (this.preview) {
       this.preview.columnWidths[columnIdx] = width;
    }
  }

  getPreviewQuery() {
    return this.getState().previewQuery;
  }

  getResultsPreview() {
    return this.getState().preview;
  }
}

export default alt.createStore(ResultsPreviewStore, 'ResultsPreviewStore');
