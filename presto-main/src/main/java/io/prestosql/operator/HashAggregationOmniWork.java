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
import nova.hetu.omnicache.runtime.OmniOpStep;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import static com.google.common.base.Preconditions.checkState;

public final class HashAggregationOmniWork<O>
        implements Work<Vec[]>
{

    OmniRuntime omniRuntime;
    String compileID;
    private boolean finished;
    private Vec[] result;
    private Page page;
    String omniKey;
    VecType[] outTypes;

    public HashAggregationOmniWork(Page page, OmniRuntime omniRuntime, String compileID, String omniKey)
    {
        this.page = page;
        this.omniRuntime = omniRuntime;
        this.compileID = compileID;
        this.omniKey = omniKey;
    }

    @Override
    public boolean process()
    {
        Vec[] inputData = new Vec[2];
        inputData[0] = (LongVec) page.getBlock(0).getValuesVec();
        inputData[1] = (LongVec) page.getBlock(1).getValuesVec();

        System.out.println("before omni execute-------");
        LongVec column0 = (LongVec) inputData[0];
        LongVec column1 = (LongVec) inputData[1];
        for (int i = 0; i < inputData[0].size(); i++) {
            System.out.println(column0.get(i) + " " + column1.get(i));
        }

        int rowNum = page.getPositionCount();

        outTypes = new VecType[] {VecType.LONG, VecType.LONG};

        omniRuntime.execute(compileID, omniKey, inputData, rowNum, outTypes, OmniOpStep.INTERMEDIATE);

        finished = true;
        return true;
    }

    @Override
    public Vec[] getResult()
    {
        checkState(finished, "process has not finished");
        long start = System.currentTimeMillis();

        result = (Vec[]) omniRuntime.execute(compileID, omniKey, null, 0, outTypes, OmniOpStep.FINAL);
        Vec[] vecs = result;

        System.out.println("after omni execute-------");
        LongVec column0 = (LongVec) vecs[0];
        LongVec column1 = (LongVec) vecs[1];
        for (int i = 0; i < vecs[0].size(); i++) {
            System.out.println(column0.get(i) + " " + column1.get(i));
        }
        System.out.println("omni runtime final execute:" + (System.currentTimeMillis() - start));
        return result;
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
