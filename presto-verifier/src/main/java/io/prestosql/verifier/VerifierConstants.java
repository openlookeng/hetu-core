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
package io.prestosql.verifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VerifierConstants
{
    /**
     * Jdbc driver list
     */
    public static final List<String> variableJdbcList = Collections.unmodifiableList(new ArrayList<String>() {
        {
            this.add("com.sap.db.jdbc.Driver");
            this.add("oracle.jdbc.driver.OracleDriver");
            this.add("com.microsoft.jdbc.sqlserver.SQLServerDriver");
            this.add("com.mysql.jdbc.Driver");
            this.add("org.postgresql.Driver");
            this.add("org.h2.Driver");
        }
    });

    private VerifierConstants()
    {
    }
}
