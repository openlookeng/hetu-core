/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.oracle;

import java.io.Closeable;
import java.security.SecureRandom;

import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;

public class TestSynonym
        implements Closeable
{
    private final TestingOracleServer oracleServer;
    private final String name;

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 5;

    public TestSynonym(TestingOracleServer oracleServer, String namePrefix, String definition)
    {
        this.oracleServer = oracleServer;
        this.name = namePrefix + "_" + randomTableSuffix();
        try {
            oracleServer.execute(format("CREATE SYNONYM %s %s", name, definition));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getName()
    {
        return name;
    }

    public String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(RANDOM.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }

    @Override
    public void close()
    {
        try {
            oracleServer.execute("DROP SYNONYM " + name);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
