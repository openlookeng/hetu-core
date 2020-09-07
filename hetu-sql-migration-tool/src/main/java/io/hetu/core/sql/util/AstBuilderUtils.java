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

package io.hetu.core.sql.util;

import io.hetu.core.sql.migration.parser.ErrorType;
import io.hetu.core.sql.migration.parser.UnsupportedException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.tree.NodeLocation;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import static java.util.Objects.requireNonNull;

public class AstBuilderUtils
{
    private AstBuilderUtils(){}

    public static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    public static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }

    public static UnsupportedException unsupportedError(ErrorType errorType, String errorMessage, ParserRuleContext context)
    {
        return new UnsupportedException(errorType.getValue() + ": " + errorMessage, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }

    public static UnsupportedException unsupportedError(ErrorType errorType, String errorMessage)
    {
        return new UnsupportedException(errorType.getValue() + ": " + errorMessage);
    }
}
