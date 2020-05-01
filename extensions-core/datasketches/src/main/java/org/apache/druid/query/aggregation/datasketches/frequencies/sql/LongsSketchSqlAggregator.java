/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.datasketches.frequencies.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LongsSketchSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new LongsSketchSqlAggFunction();
  private static final String NAME = "APPROX_FREQUENCY_LONGS";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    final RexNode columnRexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        aggregateCall.getArgList().get(0)
    );

    final DruidExpression columnArg = Expressions.toDruidExpression(plannerContext, rowSignature, columnRexNode);
    if (columnArg == null) {
      return null;
    }

    final int maxMapSize;
    if (aggregateCall.getArgList().size() >= 2) {
      final RexNode maxMapSizeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(1)
      );

      if (!maxMapSizeArg.isA(SqlKind.LITERAL)) {
        // maxMapSize must be a literal in order to plan.
        return null;
      }

      maxMapSize = ((Number) RexLiteral.value(maxMapSizeArg)).intValue();
    } else {
      maxMapSize = LongsSketchAggregatorFactory.DEFAULT_MAX_MAP_SIZE;
    }

    final String errorType;
    if (aggregateCall.getArgList().size() >= 3) {
      final RexNode errorTypeArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(2)
      );

      if (!errorTypeArg.isA(SqlKind.LITERAL)) {
        // errorType must be a literal in order to plan.
        return null;
      }

      errorType = RexLiteral.stringValue(errorTypeArg);
    } else {
      errorType = LongsSketchAggregatorFactory.DEFAULT_ERROR_TYPE.name();
    }

    final long threshold;
    if (aggregateCall.getArgList().size() >= 4) {
      final RexNode thresholdArg = Expressions.fromFieldAccess(
          rowSignature,
          project,
          aggregateCall.getArgList().get(3)
      );

      if (!thresholdArg.isA(SqlKind.LITERAL)) {
        // threshold must be a literal in order to plan.
        return null;
      }

      threshold = ((Number) RexLiteral.value(thresholdArg)).longValue();
    } else {
      threshold = LongsSketchWrap.DEFAULT_THRESHOLD;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final AggregatorFactory aggregatorFactory;
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    if (columnArg.isDirectColumnAccess() && rowSignature.getColumnType(columnArg.getDirectColumn()) == ValueType.COMPLEX) {
      aggregatorFactory = new LongsSketchMergeAggregatorFactory(aggregatorName, columnArg.getDirectColumn(), maxMapSize, errorType, threshold);
    } else {
      final SqlTypeName sqlTypeName = columnRexNode.getType().getSqlTypeName();
      final ValueType inputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (inputType == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, aggregatorName);
      }

      final DimensionSpec dimensionSpec;

      if (columnArg.isDirectColumnAccess()) {
        dimensionSpec = columnArg.getSimpleExtraction().toDimensionSpec(null, inputType);
      } else {
        VirtualColumn virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            plannerContext,
            columnArg,
            sqlTypeName
        );
        dimensionSpec = new DefaultDimensionSpec(virtualColumn.getOutputName(), null, inputType);
        virtualColumns.add(virtualColumn);
      }

      aggregatorFactory = new LongsSketchBuildAggregatorFactory(
          aggregatorName,
          dimensionSpec.getDimension(),
          maxMapSize,
          errorType,
          threshold
      );
    }

    return Aggregation.create(
        virtualColumns,
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(
            name,
            aggregatorFactory.getName()
        ) : null
    );
  }

  private static class LongsSketchSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE_1 = "'" + NAME + "(column, maxMapSize, errorType, threshold)'\n";
    private static final String SIGNATURE_2 = "'" + NAME + "(column, maxMapSize, errorType)'\n";
    private static final String SIGNATURE_3 = "'" + NAME + "(column, maxMapSize)'\n";

    LongsSketchSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          OperandTypes.or(
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE_1, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE_2, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING)
              ),
              OperandTypes.and(
                  OperandTypes.sequence(SIGNATURE_3, OperandTypes.ANY, OperandTypes.LITERAL),
                  OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
              ),
              OperandTypes.ANY
          ),
          SqlFunctionCategory.STRING,
          false,
          false
      );
    }
  }
}
