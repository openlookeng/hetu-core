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
import React from "react";

class Footer
    extends React.Component {
    componentDidMount() {
    }

    render() {
        return (
            <div className='flex footer'>
                <div className='flex flex-initial'>
                    <p>
                        <a href="mailto:contact@openlookeng.io">contact@openlookeng.io</a>
                    </p>
                </div>
                <div className='flex justify-flex-end'>
                    <div className='flex flex-initial'>
                        <p>Copyright © 2021 <a href={"https://openlookeng.io"} target="_blank">openLooKeng</a>. All rights reserved</p>
                    </div>
                </div>
            </div>
        );
    }
}

export default Footer;