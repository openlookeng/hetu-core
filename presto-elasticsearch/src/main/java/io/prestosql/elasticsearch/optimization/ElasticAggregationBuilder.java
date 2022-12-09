/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.elasticsearch.optimization;

import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ElasticAggregationBuilder
{
    private ElasticSearchRowExpressionConverter elasticSearchRowExpressionConverter;

    @Inject
    public ElasticAggregationBuilder(ElasticSearchRowExpressionConverter elasticSearchRowExpressionConverter)
    {
        this.elasticSearchRowExpressionConverter = elasticSearchRowExpressionConverter;
    }

    public static boolean isOptimizationSupported(Map<Symbol, AggregationNode.Aggregation> aggregations, List<Symbol> groupingKeys)
    {
        return aggregations.entrySet().stream().allMatch(ElasticAggregationBuilder::isOptimizationSupported);
    }

    private static boolean isOptimizationSupported(Map.Entry<Symbol, AggregationNode.Aggregation> symbolAggregationEntry)
    {
        switch (symbolAggregationEntry.getValue().getFunctionCall().getDisplayName().toUpperCase(Locale.ROOT)) {
            case "SUM":
            case "COUNT":
                List<RowExpression> arguments = symbolAggregationEntry.getValue().getArguments();

                // currently push-down of aggregation is supported only for variable reference only
                boolean isAllFieldsVariableReference = arguments.stream().allMatch(argument -> argument instanceof VariableReferenceExpression);
                return isAllFieldsVariableReference;
            default:
                return false;
        }
    }

    public Optional<AggregationBuilder> getAggBuilder(Symbol symbol, AggregationNode.Aggregation aggregation, Assignments assignmentsFromSource)
    {
        // In future to support more aggregate functions add cases in the below switch
        switch (aggregation.getFunctionCall().getDisplayName().toUpperCase(Locale.ROOT)) {
            case "SUM":
                return getSumBuilder(aggregation, symbol, assignmentsFromSource);
            case "COUNT":
                return getCountBuilder(aggregation, symbol, assignmentsFromSource);
            default:
                return Optional.empty();
        }
    }

    public Optional<AggregationBuilder> getSumBuilder(AggregationNode.Aggregation aggregation, Symbol symbol, Assignments assignmentsFromSource)
    {
        List<String> aggFields = getFields(aggregation.getArguments(), assignmentsFromSource);
        if (aggFields.isEmpty()) {
            return Optional.empty();
        }
        String firstArgument = aggFields.get(0);
        return Optional.ofNullable(AggregationBuilders.sum(symbol.getName()).field(firstArgument));
    }

    public Optional<AggregationBuilder> getCountBuilder(AggregationNode.Aggregation aggregation, Symbol symbol, Assignments assignmentsFromSource)
    {
        List<String> aggFields = getFields(aggregation.getArguments(), assignmentsFromSource);
        if (aggFields.isEmpty()) {
            return Optional.empty();
        }
        String firstArgument = aggFields.get(0);
        return Optional.ofNullable(AggregationBuilders.count(symbol.getName()).field(firstArgument));
    }

    public List<String> getFields(List<RowExpression> arguments, Assignments assignments)
    {
        ElasticSearchConverterContext elasticSearchConverterContext = new ElasticSearchConverterContext();
        List<String> fields = new ArrayList<>();
        for (RowExpression argument : arguments) {
            if (argument instanceof VariableReferenceExpression) {
                String field = getField(elasticSearchConverterContext, (VariableReferenceExpression) argument, assignments);
                fields.add(field);
            }
        }
        return fields;
    }

    private String getField(ElasticSearchConverterContext elasticSearchConverterContext, VariableReferenceExpression argument, Assignments assignments)
    {
        String field = elasticSearchRowExpressionConverter.visitVariableReference(argument, elasticSearchConverterContext);
        if (elasticSearchConverterContext.isHasConversionFailed()) {
            return "";
        }
        else {
            if (assignments == null) {
                return field;
            }
            else {
                return assignments.getMap().entrySet().stream()
                        .filter(entrySet -> entrySet.getKey().getName().equals(field))
                        .map(Map.Entry::getValue).filter(Objects::nonNull)
                        .map(rowExpression -> rowExpression.accept(elasticSearchRowExpressionConverter, new ElasticSearchConverterContext()))
                        .findFirst().orElse(field);
            }
        }
    }

    public void buildAggOptimzation(ElasticAggOptimizationContext elasticAggOptimizationContext, SearchSourceBuilder sourceBuilder)
    {
        if (elasticAggOptimizationContext == null) {
            return;
        }

        Map<Symbol, AggregationNode.Aggregation> aggregations = elasticAggOptimizationContext.getAggregations();
        List<Symbol> groupingKeys = elasticAggOptimizationContext.getGroupingKeys();

        if (!isOptimizationSupported(aggregations, groupingKeys)) {
            return;
        }

        // checking if group by clause is present
        if (groupingKeys != null && !groupingKeys.isEmpty()) {
            groupingKeys = convertAssignmentsIfPresent(groupingKeys, elasticAggOptimizationContext.getAssignmentsFromSource());
            Symbol firstGroupingKey = groupingKeys.get(0);
            TermsAggregationBuilder groupByClauseAggBuilder = AggregationBuilders.terms("group_by_clause_" + getEscapedFieldName(firstGroupingKey));
            groupByClauseAggBuilder.field(firstGroupingKey.getName());
            sourceBuilder.aggregation(groupByClauseAggBuilder);

            if (groupingKeys.size() > 1) {
                for (Symbol groupingKey : groupingKeys.subList(1, groupingKeys.size())) {
                    TermsAggregationBuilder groupByClauseSubAggBuilder = AggregationBuilders.terms("group_by_clause_" + getEscapedFieldName(groupingKey));
                    groupByClauseSubAggBuilder.field(groupingKey.getName());
                    groupByClauseAggBuilder.subAggregation(groupByClauseSubAggBuilder);
                    groupByClauseAggBuilder = groupByClauseSubAggBuilder;
                }
            }
            // if group by is present the aggregations becomes sub-aggregations of the group by clause aggregation
            for (Map.Entry<Symbol, AggregationNode.Aggregation> symbolAggregationEntry : aggregations.entrySet()) {
                Optional<AggregationBuilder> aggBuilder = getAggBuilder(symbolAggregationEntry.getKey(), symbolAggregationEntry.getValue(), elasticAggOptimizationContext.getAssignmentsFromSource());
                aggBuilder.ifPresent(groupByClauseAggBuilder::subAggregation);
            }
        }
        else {
            for (Map.Entry<Symbol, AggregationNode.Aggregation> symbolAggregationEntry : aggregations.entrySet()) {
                Optional<AggregationBuilder> aggBuilder = getAggBuilder(symbolAggregationEntry.getKey(), symbolAggregationEntry.getValue(), elasticAggOptimizationContext.getAssignmentsFromSource());
                aggBuilder.ifPresent(sourceBuilder::aggregation);
            }
        }
    }

    private List<Symbol> convertAssignmentsIfPresent(List<Symbol> symbolList, Assignments assignmentsFromSource)
    {
        if (assignmentsFromSource == null) {
            return symbolList;
        }
        return symbolList.stream().map(symbol -> {
            if (assignmentsFromSource.get(symbol) != null) {
                RowExpression rowExpression = assignmentsFromSource.get(symbol);
                String variable = rowExpression.accept(elasticSearchRowExpressionConverter, new ElasticSearchConverterContext());
                return new Symbol(variable);
            }
            else {
                return symbol;
            }
        }).collect(Collectors.toList());
    }

    public static String getEscapedFieldName(Symbol groupingKeys)
    {
        return getEscapedFieldName(groupingKeys.getName());
    }

    public static String getEscapedFieldName(String name)
    {
        return name.replaceAll("[.]", "_");
    }
}
