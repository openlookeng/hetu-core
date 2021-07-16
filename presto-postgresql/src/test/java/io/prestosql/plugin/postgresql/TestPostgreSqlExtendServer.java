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
package io.prestosql.plugin.postgresql;

import java.io.IOException;

public class TestPostgreSqlExtendServer
{
    public void execute(String sql) throws Exception
    {
    }

    public void close() throws IOException
    {
    }

    public String getUser()
    {
        return "";
    }

    public String getPassWd()
    {
        return "";
    }

    public String getDatabase()
    {
        return "";
    }

    public int getPort()
    {
        return 0;
    }

    public String getJdbcUrl()
    {
        return "";
    }
}
