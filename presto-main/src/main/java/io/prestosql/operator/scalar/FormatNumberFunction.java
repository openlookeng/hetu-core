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

package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import static io.airlift.slice.Slices.utf8Slice;

public class FormatNumberFunction
{
    private static final DecimalFormat FORMAT_NUMBER_3 = new DecimalFormat("#.##");
    private static final DecimalFormat FORMAT_NUMBER_2 = new DecimalFormat("#.#");
    private static final DecimalFormat FORMAT_NUMBER_1 = new DecimalFormat("#");

    public FormatNumberFunction()
    {
        FORMAT_NUMBER_3.setRoundingMode(RoundingMode.HALF_UP);
        FORMAT_NUMBER_2.setRoundingMode(RoundingMode.HALF_UP);
        FORMAT_NUMBER_1.setRoundingMode(RoundingMode.HALF_UP);
    }

    @ScalarFunction
    @Description("Formats large number using a unit symbol")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatNumber(@SqlType(StandardTypes.BIGINT) long value)
    {
        return utf8Slice(format(value));
    }

    @ScalarFunction
    @Description("Formats large number using a unit symbol")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatNumber(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(format((long) value));
    }

    private String format(long count)
    {
        double fractional = count;
        String unit = "";
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "K";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "M";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "B";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "T";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "Q";
        }

        return getFormat(fractional).format(fractional) + unit;
    }

    private DecimalFormat getFormat(double value)
    {
        if (value < 10) {
            // show up to two decimals to get 3 significant digits
            return FORMAT_NUMBER_3;
        }
        if (value < 100) {
            // show up to one decimal to get 3 significant digits
            return FORMAT_NUMBER_2;
        }
        // show no decimals -- we have enough digits in the integer part
        return FORMAT_NUMBER_1;
    }
}
