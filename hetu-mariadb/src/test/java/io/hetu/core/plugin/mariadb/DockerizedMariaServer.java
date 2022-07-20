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
package io.hetu.core.plugin.mariadb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.docker.DockerContainer;
import io.prestosql.testing.docker.DockerContainer.HostPortProvider;
import org.testng.SkipException;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class DockerizedMariaServer
        implements Closeable
{
    private static final int MARIADB_PORT = 3306;

    private static final String MARIADB_ROOT_USER = "root";
    private static final String MARIADB_ROOT_PASSWORD = "mariarootpassword";
    private static final String MARIADB_USER = "testuser";
    private static final String MARIADB_PASSWORD = "testpassword";

    private final DockerContainer dockerContainer;

    public DockerizedMariaServer()
    {
        try {
            //pull newest mariadb image
            this.dockerContainer = new DockerContainer(
                    "mariadb:10.1",
                    ImmutableList.of(MARIADB_PORT),
                    ImmutableMap.of(
                            "MYSQL_ROOT_PASSWORD", MARIADB_ROOT_PASSWORD,
                            "MYSQL_USER", MARIADB_USER,
                            "MYSQL_PASSWORD", MARIADB_PASSWORD,
                            "MYSQL_DATABASE", "tpch"),
                    DockerizedMariaServer::healthCheck);
        }
        catch (Exception e) {
            System.out.println("## Docker environment not properly set up. Skip test. ##");
            System.out.println("Error message: " + e.getStackTrace());
            throw new SkipException("Docker environment not initialized for tests");
        }
    }

    private static void healthCheck(HostPortProvider hostPortProvider)
            throws SQLException
    {
        String jdbcUrl = getJdbcUrl(hostPortProvider, MARIADB_ROOT_USER, MARIADB_ROOT_PASSWORD);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT 1");
        }
    }

    public void init()
            throws SQLException
    {
        String jdbcUrl = getJdbcUrl(dockerContainer::getHostPort, MARIADB_ROOT_USER, MARIADB_ROOT_PASSWORD);
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.execute(format("GRANT ALL PRIVILEGES ON *.* TO '%s'", MARIADB_USER));
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(dockerContainer::getHostPort, MARIADB_USER, MARIADB_PASSWORD);
    }

    private static String getJdbcUrl(HostPortProvider hostPortProvider, String user, String password)
    {
        //As for 2.x MariaDB Connector version,the jdbcURL prefix can be both jdbc://mariadb and jdbc://mysql
        return format("jdbc:mariadb://localhost:%s?user=%s&password=%s&useSSL=false&allowPublicKeyRetrieval=true", hostPortProvider.getHostPort(MARIADB_PORT), user, password);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
