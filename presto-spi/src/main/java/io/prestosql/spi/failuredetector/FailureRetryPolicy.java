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
package io.prestosql.spi.failuredetector;

import io.prestosql.spi.HostAddress;

public interface FailureRetryPolicy
{
    IBackoff getBackoff();

    boolean hasFailed(HostAddress address);

    String FD_RETRY_TYPE = "fd.retry.type";
    String FD_RETRY_PROFILE = "fd.retry.profile";
    String MAX_RETRY_COUNT = "max.retry.count";
    String MAX_TIMEOUT_DURATION = "max.error.duration";

    int DEFAULT_RETRY_COUNT = 100;
    String DEFAULT_TIMEOUT_DURATION = "300s";

    // FailureRetryPolicy type names, to be used in profile parameter fd.retry.type
    String TIMEOUT = "timeout";
    String MAXRETRY = "max-retry";
}
