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
package io.hetu.core.plugin.exchange.filesystem.util;

import io.airlift.log.Logger;
import io.hetu.core.plugin.exchange.filesystem.FileSystemExchangeSink;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import static io.hetu.core.plugin.exchange.filesystem.util.CheckSumUtils.CheckSumAlgorithms.SHA512;
import static io.hetu.core.plugin.exchange.filesystem.util.CheckSumUtils.checksum;
import static org.testng.Assert.assertEquals;

public class CheckSumUtilsTest
{
    private static final Logger log = Logger.get(FileSystemExchangeSink.class);

    @Test
    public void testChecksum()
    {
        String filepath = "/opt/hadoop-3.2.4-src.tar.gz";
        try {
            final String expected = "3b346cdb144b61b45d1fb0cc0ebc45cb61c9a9dcc125930456c9a6b47e96b9ca9507767b5e4524d7c1d4fe07e6a046f7e1c1ee976d16e02d52671528ce7d1246";
            String checksum = checksum(filepath, SHA512);
            assertEquals(checksum, expected);
        }
        catch (NoSuchAlgorithmException e) {
            log.error(e);
        }
    }

    @Test
    public void testTestChecksum() throws NoSuchAlgorithmException
    {
        String expected = "61fdf527eb4a1a793633ea745c36ae06f197b565f07ea0e2254c15064bd8c744d8e66b73c55b409b3dbcb3c3cf4f52d3f234e3dfd7cd4a344bb8d83bbf0094db";
        String src = "input string";
        String checksum = checksum(src.getBytes(StandardCharsets.US_ASCII), SHA512);
        assertEquals(checksum, expected);
    }
}
