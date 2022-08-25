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
package org.apache.orc;

import java.util.Date;

/**
 * Statistics for DATE columns.
 */
public interface DateColumnStatistics
        extends ColumnStatistics
{
    default long getMinimumDayOfEpoch()
    {
        return 0L;
    }

    default long getMaximumDayOfEpoch()
    {
        return 0L;
    }

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    Date getMinimum();

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    Date getMaximum();
}
