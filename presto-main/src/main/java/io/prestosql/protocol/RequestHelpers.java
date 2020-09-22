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
package io.prestosql.protocol;

import io.airlift.http.client.Request.Builder;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

public class RequestHelpers
{
    private RequestHelpers()
    {
    }

    /**
     * Sets the request Content-Type/Accept headers for JSON or SMILE encoding based on the
     * given isBinaryTransportEnabled argument.
     */
    public static Builder setContentTypeHeaders(boolean isBinaryEncoding, Builder requestBuilder)
    {
        if (isBinaryEncoding) {
            return requestBuilder
                    .setHeader(CONTENT_TYPE, SmileHeader.APPLICATION_JACKSON_SMILE)
                    .setHeader(ACCEPT, SmileHeader.APPLICATION_JACKSON_SMILE);
        }
        return requestBuilder
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString());
    }
}
