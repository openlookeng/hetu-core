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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public interface IBackoff
{
    int MIN_RETRIES = 3;

    List<Duration> DEFAULT_BACKOFF_DELAY_INTERVALS = ImmutableList.<Duration>builder()
            .add(new Duration(0, MILLISECONDS))
            .add(new Duration(50, MILLISECONDS))
            .add(new Duration(100, MILLISECONDS))
            .add(new Duration(200, MILLISECONDS))
            .add(new Duration(500, MILLISECONDS))
            .build();

    boolean failure();

    void startRequest();

    long getBackoffDelayNanos();

    void success();

    long getFailureCount();

    Duration getFailureDuration();

    Duration getFailureRequestTimeTotal();
}
