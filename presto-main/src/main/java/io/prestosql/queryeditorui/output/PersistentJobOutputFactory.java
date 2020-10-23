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
package io.prestosql.queryeditorui.output;

import com.google.inject.Inject;

import java.net.URI;
import java.util.UUID;

public class PersistentJobOutputFactory
{
    @Inject
    public PersistentJobOutputFactory()
    {
    }

    public PersistentJobOutput create(final String tmpTable, final UUID jobUUID)
    {
        return new CSVPersistentOutput(null, "csv", null);
    }

    public static PersistentJobOutput create(String type, String description, URI location)
    {
        if (location == null) {
            return null;
        }
        else if (location.isAbsolute()) {
            return new CSVPersistentOutput(location, type, description);
        }
        else {
            return null;
        }
    }
}
