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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DoubleArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Type;
import nova.hetu.omnicache.runtime.OmniOpStep;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public final class HashAggregationOmniWork<O>
        implements Work<Page>
{

    OmniRuntime omniRuntime;
    List<String> compileID;
    private boolean finished;
    private Vec[] result;
    private Page page;
    String omniKey;
    VecType[] outTypes;

    public HashAggregationOmniWork(Page page, OmniRuntime omniRuntime, List<String> compileID, String omniKey)
    {
        this.page = page;
        this.omniRuntime = omniRuntime;
        this.compileID = compileID;
        this.omniKey = omniKey;
    }

    @Override
    public boolean process()
    {
        int channelCount = page.getChannelCount();
        Vec[] inputData = new Vec[channelCount];
        for (int i = 0; i < channelCount; i++) {
            inputData[i] = page.getBlock(i).getValues();
        }
//        LongVec
//
        int rowNum = page.getPositionCount();

        outTypes = new VecType[] {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG};

        if (inputData[channelCount - 1] instanceof LongVec) {
            omniRuntime.execute(compileID.get(0), omniKey, inputData, rowNum, outTypes, OmniOpStep.INTERMEDIATE);
        }
        else {
            omniRuntime.execute(compileID.get(1), omniKey, inputData, rowNum, outTypes, OmniOpStep.INTERMEDIATE);
        }

        finished = true;
        return true;
    }

    @Override
    public Page getResult()
    {
        checkState(finished, "process has not finished");

        result = (Vec[]) omniRuntime.getResults(omniKey, outTypes);

        return toResult(result);
    }

    public Page toResult(Vec[] omniExecutionResult)
    {
        int positionCount = omniExecutionResult[0].size();
        int chanelCount = omniExecutionResult.length;

        boolean[] valueIsNull = new boolean[positionCount];
        for (int i = 0; i < positionCount; i++) {
            valueIsNull[i] = false;
        }
        Block[] blocks = new Block[chanelCount];
        for (int i = 0; i < chanelCount; i++) {
            if (omniExecutionResult[i] instanceof DoubleVec) {
                blocks[i] = new DoubleArrayBlock(positionCount, Optional.of(valueIsNull), ((DoubleVec) omniExecutionResult[i]));
            }
            else {
                blocks[i] = new LongArrayBlock(positionCount, Optional.of(valueIsNull), (LongVec) omniExecutionResult[i]);
            }
        }

        Page page = new Page(blocks);

        return page;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public void updatePages(Page page)
    {
        this.page = page;
    }
}
