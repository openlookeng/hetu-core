/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive.util;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;

public class OperatorUtils
{
    private OperatorUtils() {}

    public static final GenericUDF getGenericUDFUsingOperator(String operator)
    {
        if (operator.equalsIgnoreCase("=")) {
            return new GenericUDFOPEqual();
        }
        if (operator.equalsIgnoreCase(">=")) {
            return new GenericUDFOPEqualOrGreaterThan();
        }
        if (operator.equalsIgnoreCase("<=")) {
            return new GenericUDFOPEqualOrLessThan();
        }
        if (operator.equalsIgnoreCase(">")) {
            return new GenericUDFOPGreaterThan();
        }
        if (operator.equalsIgnoreCase("<")) {
            return new GenericUDFOPLessThan();
        }
        if (operator.equalsIgnoreCase("<>")) {
            return new GenericUDFOPNotEqual();
        }
        else {
            throw new UnsupportedOperationException("Operator " + operator + " not implemented");
        }
    }
}
