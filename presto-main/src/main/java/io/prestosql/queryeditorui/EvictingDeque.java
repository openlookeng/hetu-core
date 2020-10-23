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
package io.prestosql.queryeditorui;

import com.google.common.collect.ForwardingBlockingDeque;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class EvictingDeque<E>
        extends ForwardingBlockingDeque<E>
{
    private final LinkedBlockingDeque<E> blockingDeque;

    public EvictingDeque(final int capacity)
    {
        this.blockingDeque = new LinkedBlockingDeque<>(capacity);
    }

    @Override
    public void put(E e) throws InterruptedException
    {
        this.add(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException
    {
        return this.add(e);
    }

    @Override
    public boolean add(E element)
    {
        final boolean initialResult = blockingDeque.offer(element);
        return initialResult || (evictItem(blockingDeque) && add(element));
    }

    @Override
    protected BlockingDeque<E> delegate()
    {
        return blockingDeque;
    }

    protected boolean evictItem(LinkedBlockingDeque<E> deque)
    {
        return deque.remove() != null;
    }
}
