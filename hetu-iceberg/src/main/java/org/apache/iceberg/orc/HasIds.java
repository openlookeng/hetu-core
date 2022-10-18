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
package org.apache.iceberg.orc;

import org.apache.orc.TypeDescription;

import java.util.List;
import java.util.function.Predicate;

class HasIds
        extends OrcSchemaVisitor<Boolean>
{
    @Override
    public Boolean record(TypeDescription record, List<String> names, List<Boolean> fields)
    {
        return ORCSchemaUtil.icebergID(record).isPresent() || fields.stream().anyMatch(Predicate.isEqual(true));
    }

    @Override
    public Boolean list(TypeDescription array, Boolean element)
    {
        return ORCSchemaUtil.icebergID(array).isPresent() || element;
    }

    @Override
    public Boolean map(TypeDescription map, Boolean key, Boolean value)
    {
        return ORCSchemaUtil.icebergID(map).isPresent() || key || value;
    }

    @Override
    public Boolean primitive(TypeDescription primitive)
    {
        return ORCSchemaUtil.icebergID(primitive).isPresent();
    }
}
