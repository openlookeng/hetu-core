package io.prestosql.elasticsearch.optimization;

import io.prestosql.spi.relation.*;
import io.prestosql.spi.sql.RowExpressionConverter;

public class ElasticSearchRowExpressionConverter implements RowExpressionConverter<ElasticSearchConverterContext> {

    private final String EMPTY_STRING = "";

    @Override
    public String visitCall(CallExpression call, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitCall(call, context);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitSpecialForm(specialForm, context);
    }

    @Override
    public String visitConstant(ConstantExpression literal, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitConstant(literal, context);
    }

    @Override
    public String visitVariableReference(VariableReferenceExpression reference, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitVariableReference(reference, context);
    }

    @Override
    public String visitInputReference(InputReferenceExpression reference, ElasticSearchConverterContext context) {
        return handleUnsupportedOptimize(context);
    }

    @Override
    public String visitLambda(LambdaDefinitionExpression lambda, ElasticSearchConverterContext context) {
        return handleUnsupportedOptimize(context);
    }

    public String handleUnsupportedOptimize(ElasticSearchConverterContext context){
        context.setConversionFailed();
        return EMPTY_STRING;
    }
}
