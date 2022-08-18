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
package io.prestosql.spi.exchange;

import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface ExchangeSource
        extends Closeable
{
    CompletableFuture<Void> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    CompletableFuture<?> isBlocked();

    boolean isFinished();

    @Nullable
    Slice read();

    long getMemoryUsage();

    @Override
    void close();
}
