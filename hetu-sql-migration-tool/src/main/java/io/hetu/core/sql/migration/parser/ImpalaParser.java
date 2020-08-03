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
package io.hetu.core.sql.migration.parser;

import io.hetu.core.migration.source.impala.ImpalaSqlBaseListener;
import io.hetu.core.migration.source.impala.ImpalaSqlLexer;
import io.hetu.core.migration.source.impala.ImpalaSqlParser;
import io.hetu.core.sql.migration.tool.ConvertionOptions;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.CaseInsensitiveStream;
import io.prestosql.sql.parser.ErrorHandler;
import io.prestosql.sql.parser.IdentifierSymbol;
import io.prestosql.sql.parser.ParsingException;
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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.hetu.core.sql.migration.SqlSyntaxType.IMPALA;
import static java.util.Objects.requireNonNull;

public class ImpalaParser
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
            .specialRule(ImpalaSqlParser.RULE_expression, "<expression>")
            .specialRule(ImpalaSqlParser.RULE_booleanExpression, "<expression>")
            .specialRule(ImpalaSqlParser.RULE_valueExpression, "<expression>")
            .specialRule(ImpalaSqlParser.RULE_primaryExpression, "<expression>")
            .specialRule(ImpalaSqlParser.RULE_identifier, "<identifier>")
            .specialRule(ImpalaSqlParser.RULE_string, "<string>")
            .specialRule(ImpalaSqlParser.RULE_query, "<query>")
            .specialRule(ImpalaSqlParser.RULE_type, "<type>")
            .specialToken(ImpalaSqlParser.INTEGER_VALUE, "<integer>")
            .ignoredRule(ImpalaSqlParser.RULE_nonReserved)
            .build();

    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols;
    private boolean enhancedErrorHandlerEnabled;

    public ImpalaParser()
    {
        this(new SqlParserOptions());
    }

    @Inject
    public ImpalaParser(SqlParserOptions options)
    {
        requireNonNull(options, "options is null");
        allowedIdentifierSymbols = EnumSet.copyOf(options.getAllowedIdentifierSymbols());
        enhancedErrorHandlerEnabled = options.isEnhancedErrorHandlerEnabled();
    }

    public JSONObject invokeParser(String sql, Function<ImpalaSqlParser, ParserRuleContext> parseFunction, ConvertionOptions convertionOptions)
    {
        try {
            ImpalaSqlLexer lexer = new ImpalaSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            ImpalaSqlParser parser = new ImpalaSqlParser(tokenStream);

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
            String conversionInfo = "";
            try {
                ParserRuleContext tree;
                try {
                    // first, try parsing with potentially faster SLL mode
                    parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                    tree = parseFunction.apply(parser);
                }
                catch (ParseCancellationException ex) {
                    // if we fail, parse with LL mode
                    tokenStream.reset(); // rewind input stream
                    parser.reset();

                    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                    tree = parseFunction.apply(parser);
                }

                ImpalaAstBuilder impalaAstBuilder = new ImpalaAstBuilder(convertionOptions);
                Statement statement = (Statement) impalaAstBuilder.visit(tree);
                if (statement == null) {
                    conversionStatus = Constants.FAIL;
                    conversionInfo = "The input sql is not valid or empty.";
                }
                else {
                    convertedSql = SqlFormatter.formatSql(statement, Optional.empty());
                    if (impalaAstBuilder.getConversionInfo().isEmpty()) {
                        conversionStatus = Constants.SUCCESS;
                    }
                    else {
                        conversionStatus = Constants.WARNING;
                        conversionInfo = String.join(",", impalaAstBuilder.getConversionInfo());
                    }
                }
            }
            catch (ParsingException | IllegalArgumentException | UnsupportedOperationException e) {
                conversionInfo = e.getMessage();
                conversionStatus = Constants.FAIL;
            }

            // Construct json format result
            JSONObject result = new JSONObject();
            try {
                result.put(Constants.ORIGINAL_SQL, sql);
                result.put(Constants.ORIGINAL_SQL_TYPE, IMPALA);
                result.put(Constants.CONVERTED_SQL, convertedSql);
                result.put(Constants.STATUS, conversionStatus);
                result.put(Constants.MESSAGE, conversionInfo);
            }
            catch (JSONException e) {
                throw new ParsingException("Construct parsing result failed." + e.getMessage());
            }

            return result;
        }
        catch (StackOverflowError e) {
            throw new ParsingException("statement is too large (stack overflow while parsing)");
        }
    }

    private class PostProcessor
            extends ImpalaSqlBaseListener
    {
        private final List<String> ruleNames;

        public PostProcessor(List<String> ruleNames)
        {
            this.ruleNames = ruleNames;
        }

        @Override
        public void exitQuotedIdentifier(ImpalaSqlParser.QuotedIdentifierContext context)
        {
            Token token = context.STRING().getSymbol();
            if (token.getText().length() == 2) { // empty identifier
                throw new ParsingException("Zero-length delimited identifier not allowed", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitUnquotedIdentifier(ImpalaSqlParser.UnquotedIdentifierContext context)
        {
            String identifier = context.IDENTIFIER().getText();
            for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedIdentifierSymbols)) {
                char symbol = identifierSymbol.getSymbol();
                if (identifier.indexOf(symbol) >= 0) {
                    throw new ParsingException("identifiers must not contain '" + identifierSymbol.getSymbol() + "'", null, context.IDENTIFIER().getSymbol().getLine(), context.IDENTIFIER().getSymbol().getCharPositionInLine());
                }
            }
        }

        @Override
        public void exitBackQuotedIdentifier(ImpalaSqlParser.BackQuotedIdentifierContext context)
        {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            if (token.getText().length() == 2) { // empty identifier
                throw new ParsingException("Zero-length delimited identifier not allowed", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitDigitIdentifier(ImpalaSqlParser.DigitIdentifierContext context)
        {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; surround the identifier with double quotes",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitNonReserved(ImpalaSqlParser.NonReservedContext context)
        {
            // we can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
            // Otherwise, ANTLR gets confused an fires spurious notifications.
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved words with IDENT tokens
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    ImpalaSqlLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex()));
        }
    }
}
