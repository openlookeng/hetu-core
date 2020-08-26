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
package io.hetu.core.sql.migration.parser;

import io.hetu.core.migration.source.hive.HiveSqlBaseListener;
import io.hetu.core.migration.source.hive.HiveSqlLexer;
import io.hetu.core.migration.source.hive.HiveSqlParser;
import io.hetu.core.sql.migration.Constants;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.CaseInsensitiveStream;
import io.prestosql.sql.parser.ErrorHandler;
import io.prestosql.sql.parser.IdentifierSymbol;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.tree.Statement;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.hetu.core.sql.migration.SqlSyntaxType.HIVE;
import static java.util.Objects.requireNonNull;

public class HiveParser
{
    private static final BaseErrorListener LEXER_ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    private static final ErrorHandler PARSER_ERROR_HANDLER = ErrorHandler.builder()
            .specialRule(HiveSqlParser.RULE_expression, "<expression>")
            .specialRule(HiveSqlParser.RULE_booleanExpression, "<expression>")
            .specialRule(HiveSqlParser.RULE_valueExpression, "<expression>")
            .specialRule(HiveSqlParser.RULE_primaryExpression, "<expression>")
            .specialRule(HiveSqlParser.RULE_identifier, "<identifier>")
            .specialRule(HiveSqlParser.RULE_string, "<string>")
            .specialRule(HiveSqlParser.RULE_query, "<query>")
            .specialRule(HiveSqlParser.RULE_type, "<type>")
            .specialToken(HiveSqlLexer.INTEGER_VALUE, "<integer>")
            .ignoredRule(HiveSqlParser.RULE_nonReserved)
            .build();

    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols;
    private boolean enhancedErrorHandlerEnabled;

    public HiveParser()
    {
        this(new SqlParserOptions());
    }

    @Inject
    public HiveParser(SqlParserOptions options)
    {
        requireNonNull(options, "options is null");
        allowedIdentifierSymbols = EnumSet.copyOf(options.getAllowedIdentifierSymbols());
        enhancedErrorHandlerEnabled = options.isEnhancedErrorHandlerEnabled();
    }

    public JSONObject invokeParser(String sql, Function<HiveSqlParser, ParserRuleContext> parseFunction, ParsingOptions parsingOptions)
    {
        try {
            HiveSqlLexer lexer = new HiveSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            HiveSqlParser parser = new HiveSqlParser(tokenStream);

            // Override the default error strategy to not attempt inserting or deleting a token.
            // Otherwise, it messes up error reporting
            parser.setErrorHandler(new DefaultErrorStrategy()
            {
                @Override
                public Token recoverInline(Parser recognizer)
                        throws RecognitionException
                {
                    if (nextTokensContext == null) {
                        throw new InputMismatchException(recognizer);
                    }
                    else {
                        throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                    }
                }
            });

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            lexer.removeErrorListeners();
            lexer.addErrorListener(LEXER_ERROR_LISTENER);

            parser.removeErrorListeners();

            if (enhancedErrorHandlerEnabled) {
                parser.addErrorListener(PARSER_ERROR_HANDLER);
            }
            else {
                parser.addErrorListener(LEXER_ERROR_LISTENER);
            }

            String convertedSql = "";
            String conversionStatus = "";
            String errorMessage = "";
            JSONArray diffArray = new JSONArray();
            HiveAstBuilder hiveAstBuilder = null;
            try {
                ParserRuleContext tree;
                try {
                    parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                    tree = parseFunction.apply(parser);
                }
                catch (ParseCancellationException e) {
                    tokenStream.reset();
                    parser.reset();
                    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                    tree = parseFunction.apply(parser);
                }

                hiveAstBuilder = new HiveAstBuilder(parsingOptions);
                Statement statement = (Statement) hiveAstBuilder.visit(tree);
                if (statement == null) {
                    conversionStatus = Constants.FAILED;
                    errorMessage = "The input sql is not valid or empty.";
                }
                else {
                    convertedSql = SqlFormatter.formatSql(statement, Optional.empty());
                    if (hiveAstBuilder.getParserDiffsList().isEmpty()) {
                        conversionStatus = Constants.SUCCESS;
                    }
                    else {
                        conversionStatus = Constants.SUCCESS;
                        for (ParserDiffs diffs : hiveAstBuilder.getParserDiffsList()) {
                            if (diffs.getDiffType().equals(DiffType.DELETED) || diffs.getDiffType().equals(DiffType.FUNCTION_WARNING)) {
                                conversionStatus = Constants.WARNING;
                            }
                            diffArray.put(diffs.toJsonObject());
                        }
                    }
                }
            }
            catch (UnsupportedException e) {
                // handle the unsupported keywords
                conversionStatus = Constants.UNSUPPORTED;
                if (hiveAstBuilder != null) {
                    for (ParserDiffs diffs : hiveAstBuilder.getParserDiffsList()) {
                        if (diffs.getDiffType().equals(DiffType.UNSUPPORTED)) {
                            diffArray.put(diffs.toJsonObject());
                            errorMessage += diffs.getMessage().isPresent() ? diffs.getMessage().get() : "";
                        }
                    }
                }
                if (errorMessage.isEmpty()) {
                    errorMessage = e.getMessage();
                }
            }
            catch (IllegalArgumentException | UnsupportedOperationException | ParsingException e) {
                errorMessage = e.getMessage();
                conversionStatus = Constants.FAILED;
            }

            // Construct json format result
            JSONObject result = new JSONObject();
            result.put(Constants.ORIGINAL_SQL, sql);
            result.put(Constants.ORIGINAL_SQL_TYPE, HIVE);
            result.put(Constants.CONVERTED_SQL, convertedSql);
            result.put(Constants.STATUS, conversionStatus);
            result.put(Constants.MESSAGE, errorMessage);
            result.put(Constants.DIFFS, diffArray);
            return result;
        }
        catch (JSONException e) {
            throw new ParsingException("Construct parsing result failed." + e.getMessage());
        }
        catch (StackOverflowError e) {
            throw new ParsingException("statement is too large (stack overflow while parsing)");
        }
    }

    private class PostProcessor
            extends HiveSqlBaseListener
    {
        private final List<String> ruleNames;

        public PostProcessor(List<String> ruleNames)
        {
            this.ruleNames = ruleNames;
        }

        @Override
        public void exitQuotedIdentifier(HiveSqlParser.QuotedIdentifierContext context)
        {
            Token token = context.STRING().getSymbol();
            if (token.getText().length() == 2) {
                throw new ParsingException("Zero-length quoted identifier is not allowed", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitUnquotedIdentifier(HiveSqlParser.UnquotedIdentifierContext context)
        {
            String identifier = context.IDENTIFIER().getText();
            for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedIdentifierSymbols)) {
                if (identifier.indexOf(identifierSymbol.getSymbol()) >= 0) {
                    throw new ParsingException("Unquoted identifier contain illegal symbol '" + identifierSymbol.getSymbol() + "'",
                            null, context.IDENTIFIER().getSymbol().getLine(), context.IDENTIFIER().getSymbol().getCharPositionInLine());
                }
            }
        }

        @Override
        public void exitBackQuotedIdentifier(HiveSqlParser.BackQuotedIdentifierContext context)
        {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            if (token.getText().length() == 2) {
                throw new ParsingException("Zero-length back quoted identifier is not allowed", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitNonReserved(HiveSqlParser.NonReservedContext context)
        {
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved identify is not a terminal node, found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved identify with unquoted identify
            context.getParent().removeLastChild();
            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(new Pair<>(token.getTokenSource(), token.getInputStream()),
                    HiveSqlLexer.IDENTIFIER, token.getChannel(), token.getStartIndex(), token.getStopIndex()));
        }
    }
}
