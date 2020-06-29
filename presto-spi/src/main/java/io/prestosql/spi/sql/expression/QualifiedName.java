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
package io.prestosql.spi.sql.expression;

import java.util.List;
import java.util.StringJoiner;

public class QualifiedName
{
    private final List<String> parts;

    public QualifiedName(List<String> parts)
    {
        this.parts = parts;
    }

    public List<String> getParts()
    {
        return parts;
    }

    public String getSuffix()
    {
        return parts.get(parts.size() - 1);
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(".");
        parts.forEach(joiner::add);
        return joiner.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode()
    {
        return parts.hashCode();
    }
}
