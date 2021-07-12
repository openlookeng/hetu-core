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
package io.prestosql.cli;

import io.airlift.log.Logger;
import sun.misc.Signal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RowBufferHandler
{
    private static final Signal SIGINT = new Signal("INT");

    private static final Logger log = Logger.get(Query.class);

    private final List<List<?>> rowBuffer;
    private final CubeConsole cubeConsole;
    private int startIteration;
    private int endIteration;
    private boolean isCompleteTraversal;
    private boolean initialIterationProcess = true;
    private static long minimumRealisticBatchSize = 2L;

    private final List<List<?>> rowBufferIterationItems = new ArrayList<>();

    public List<List<?>> getRowBuffer()
    {
        return rowBuffer;
    }

    public RowBufferHandler(CubeConsole cubeConsole, List<List<?>> rowBuffer)
    {
        this.rowBuffer = rowBuffer;
        this.cubeConsole = cubeConsole;
    }

    public int getRowBufferSize()
    {
        return rowBuffer.size();
    }

    public List<?> getStartAndEndIteration()
    {
        if (isCompleteTraversal) {
            return Collections.emptyList();
        }

        if (rowBuffer.size() == 0) {
            isCompleteTraversal = true;
            return Collections.emptyList();
        }

        if (rowBuffer.size() >= 1) {
            if (rowBuffer.get(0).size() < 2) {
                isCompleteTraversal = true;
                return Collections.emptyList();
            }
        }

        if (rowBuffer.size() == 1) {
            isCompleteTraversal = true;
            String item = rowBuffer.get(0).get(0).toString();
            return Arrays.asList(item, item);
        }

        Long maxBatchProcessSize = Long.parseLong(cubeConsole.getMaxBatchProcessSize());

        if (maxBatchProcessSize < minimumRealisticBatchSize) {
            maxBatchProcessSize = minimumRealisticBatchSize;
        }

        int currentIteration;
        if (initialIterationProcess == true) {
            currentIteration = startIteration;
            initialIterationProcess = false;
        }
        else {
            startIteration = endIteration + 1;
            currentIteration = startIteration;
        }
        int totalRowsCounted = 0;
        while (currentIteration < rowBuffer.size() && (totalRowsCounted < maxBatchProcessSize)) {
            endIteration = currentIteration;
            totalRowsCounted = totalRowsCounted + Integer.parseInt(rowBuffer.get(currentIteration).get(1).toString());
            currentIteration++;
        }

        if (startIteration == endIteration || endIteration == (rowBuffer.size() - 1)) {
            isCompleteTraversal = true;
        }

        if (startIteration > endIteration) {
            isCompleteTraversal = true;
            startIteration = endIteration;
        }
        String startItem = rowBuffer.get(startIteration).get(0).toString();
        String endItem = rowBuffer.get(endIteration).get(0).toString();
        return Arrays.asList(startItem, endItem);
    }

    public void processIterationIndex() throws IOException
    {
        List<?> startAndEndIteration = getStartAndEndIteration();
        while (!startAndEndIteration.isEmpty()) {
            rowBufferIterationItems.add(startAndEndIteration);
            startAndEndIteration = getStartAndEndIteration();
        }
        startIteration = 0;
        endIteration = 0;
        flush(true);
    }

    public boolean isCompleteTraversal()
    {
        return isCompleteTraversal;
    }

    public String getItemAtPosition(int currentRow)
    {
        return rowBuffer.get(currentRow).get(0).toString();
    }

    public List<List<?>> getRowBufferIterationItems()
    {
        return rowBufferIterationItems;
    }

    private void flush(boolean complete)
            throws IOException
    {
        if (!rowBuffer.isEmpty()) {
            rowBuffer.clear();
            startIteration = 0;
            endIteration = 0;
        }
    }

    public boolean isEmptyRowBuffer()
    {
        return rowBuffer.isEmpty();
    }
}
