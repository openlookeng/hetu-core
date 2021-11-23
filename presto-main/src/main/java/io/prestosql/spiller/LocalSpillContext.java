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
package io.prestosql.spiller;

import io.prestosql.operator.SpillContext;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public final class LocalSpillContext
        implements SpillContext
{
    private final SpillContext parentSpillContext;
    private long spilledBytes;
    private boolean closed;
    private final AtomicLong spillWriteTime = new AtomicLong();
    private final AtomicLong spillReadTime = new AtomicLong();

    public LocalSpillContext(SpillContext parentSpillContext)
    {
        this.parentSpillContext = parentSpillContext;
    }

    @Override
    public synchronized void updateBytes(long bytes)
    {
        checkState(!closed, "Already closed");
        parentSpillContext.updateBytes(bytes);
        spilledBytes += bytes;
    }

    @Override
    public void updateWriteTime(long millis)
    {
        if (millis > 0) {
            spillWriteTime.addAndGet(millis);
        }
    }

    @Override
    public void updateReadTime(long millis)
    {
        if (millis > 0) {
            spillReadTime.addAndGet(millis);
        }
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }

        closed = true;
        parentSpillContext.updateBytes(-spilledBytes);
        parentSpillContext.updateReadTime(spillReadTime.get());
        parentSpillContext.updateWriteTime(spillWriteTime.get());
    }
}
