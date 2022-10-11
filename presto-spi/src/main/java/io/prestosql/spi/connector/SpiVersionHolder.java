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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

// Separate class to isolate static initialization
final class SpiVersionHolder
{
    private SpiVersionHolder() {}

    static final String SPI_VERSION;

    static {
        try {
            URL resource = ConnectorContext.class.getClassLoader().getResource("io/prestosql/spi/hetu-spi-version.txt");
            requireNonNull(resource, "version resource not found");
            try (InputStream inputStream = resource.openStream(); BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8))) {
                String spiVersion = reader.readLine();
                if (spiVersion == null || spiVersion.isEmpty() || reader.readLine() != null) {
                    throw new IllegalStateException("Malformed version resource");
                }
                SPI_VERSION = trimWhitespace(spiVersion);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String trimWhitespace(String var0)
    {
        if (var0 == null) {
            return var0;
        }
        else {
            StringBuilder var1 = new StringBuilder();
            for (int var2 = 0; var2 < var0.length(); ++var2) {
                char var3 = var0.charAt(var2);
                if (var3 != '\n' && var3 != '\f' && var3 != '\r' && var3 != '\t') {
                    var1.append(var3);
                }
            }

            return var1.toString().trim();
        }
    }
}
