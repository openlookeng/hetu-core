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
package io.prestosql.server;

import java.util.Comparator;

public enum QuerySortFilter
{
    // Creation Time
    CREATION((o1, o2) -> {
        return o1.getQueryStats().getCreateTime().compareTo(o2.getQueryStats().getCreateTime());
    }),
    // Elapsed Time
    ELAPSED((o1, o2) -> {
        return o1.getQueryStats().getElapsedTime().compareTo(o2.getQueryStats().getElapsedTime());
    }),
    // CPU Time
    CPU((o1, o2) -> {
        return o1.getQueryStats().getTotalCpuTime().compareTo(o2.getQueryStats().getTotalCpuTime());
    }),
    // Execution Time
    EXECUTION((o1, o2) -> {
        return o1.getQueryStats().getExecutionTime().compareTo(o2.getQueryStats().getExecutionTime());
    }),
    // Current Memory
    MEMORY((o1, o2) -> {
        return o1.getQueryStats().getUserMemoryReservation().compareTo(o2.getQueryStats().getUserMemoryReservation());
    }),
    // Cumulative User Memory
    CUMULATIVE((o1, o2) -> {
        double m1 = o1.getQueryStats().getCumulativeUserMemory();
        double m2 = o2.getQueryStats().getCumulativeUserMemory();
        return Double.compare(m1, m2);
    });

    private final Comparator<BasicQueryInfo> compare;

    QuerySortFilter(Comparator<BasicQueryInfo> compare)
    {
        this.compare = compare;
    }

    public Comparator<BasicQueryInfo> getCompare()
    {
        return this.compare;
    }
}
