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
import React from "react";

class ProgressBar extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        let progress = Math.round(this.props.progress);
        const progressBarStyle = {width: progress + "%", backgroundColor: "#19874e"};
        return (
            <div className="col-xs-12 query-progress-container" style={{width: "120px", padding:"0px"}}>
                <div className="progress">
                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={progress} aria-valuemin="0"
                         aria-valuemax="100" style={progressBarStyle}>
                        {progress + "%"}
                    </div>
                </div>
            </div>
        )
    }
}

export default ProgressBar;