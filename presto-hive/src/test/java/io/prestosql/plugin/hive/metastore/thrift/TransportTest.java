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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.prestosql.plugin.hive.authentication.HiveMetastoreAuthentication;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.util.Optional;

public class TransportTest
{
    @Test
    public void testCreate() throws Exception
    {
        // Setup
        final Optional<SSLContext> sslContext = Optional.of(SSLContext.getInstance("protocol"));
        final HiveMetastoreAuthentication authentication = null;

        // Run the test
        final TTransport result = Transport.create(HostAndPort.fromParts("localhost", 80), sslContext,
                Optional.of(HostAndPort.fromParts("localhost", 80)), 0, authentication);

        // Verify the results
    }
}
