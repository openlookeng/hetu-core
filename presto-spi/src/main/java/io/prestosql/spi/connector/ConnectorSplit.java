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
package io.prestosql.spi.connector;

import io.prestosql.spi.HostAddress;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

public interface ConnectorSplit
{
    boolean isRemotelyAccessible();

    List<HostAddress> getAddresses();

    default String getFilePath()
    {
        throw new NotImplementedException();
    }

    default long getStartIndex()
    {
        throw new NotImplementedException();
    }

    default long getEndIndex()
    {
        throw new NotImplementedException();
    }

    default long getLastModifiedTime()
    {
        throw new NotImplementedException();
    }

    default boolean isCacheable()
    {
        return false;
    }

    Object getInfo();

    default int getSplitCount()
    {
        return 1;
    }

    default List<ConnectorSplit> getUnwrappedSplits()
    {
        throw new NotImplementedException();
    }
}
