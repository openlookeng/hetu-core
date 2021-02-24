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
package io.prestosql.operator.aggregation.state;

import io.prestosql.spi.function.GroupedAccumulatorState;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;

public abstract class AbstractGroupedAccumulatorState
        implements GroupedAccumulatorState, Restorable
{
    private long groupId;

    @Override
    public final void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    protected final long getGroupId()
    {
        return groupId;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        return groupId;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        this.groupId = (long) state;
    }
}
