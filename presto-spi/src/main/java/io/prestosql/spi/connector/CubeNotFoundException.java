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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CubeNotFoundException
        extends NotFoundException
{
    private final String cubeName;

    public CubeNotFoundException(String cubeName)
    {
        this(cubeName, format("Cube '%s' not found", cubeName));
    }

    public CubeNotFoundException(String cubeName, String message)
    {
        super(message);
        this.cubeName = requireNonNull(cubeName, "cubeName is null");
    }

    public CubeNotFoundException(String cubeName, Throwable cause)
    {
        this(cubeName, format("Cube '%s' not found", cubeName), cause);
    }

    public CubeNotFoundException(String cubeName, String message, Throwable cause)
    {
        super(message, cause);
        this.cubeName = requireNonNull(cubeName, "cubeName is null");
    }

    public String getCubeName()
    {
        return cubeName;
    }
}
