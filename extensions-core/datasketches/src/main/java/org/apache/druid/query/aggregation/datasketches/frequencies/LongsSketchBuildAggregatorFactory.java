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

package org.apache.druid.query.aggregation.datasketches.frequencies;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * This aggregator factory is for building sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class LongsSketchBuildAggregatorFactory extends LongsSketchAggregatorFactory
{
  @JsonCreator
  public LongsSketchBuildAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxMapSize") @Nullable final Integer maxMapSize,
      @JsonProperty("errorType") @Nullable final String errorType,
      @JsonProperty("threshold") @Nullable final Long threshold
  )
  {
    super(name, fieldName, maxMapSize, errorType, threshold);
  }

  @Override
  public String getTypeName()
  {
    return LongsSketchModule.BUILD_TYPE_NAME;
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.LONGS_SKETCH_BUILD_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new LongsSketchBuildAggregator(selector, getMaxMapSize());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new LongsSketchBuildBufferAggregator(
        selector,
        getMaxMapSize(),
        getMaxIntermediateSize()
    );
  }

  /**
   * This is a convoluted way to return a list of input field names this aggregator needs.
   * Currently the returned factories are only used to obtain a field name by calling getName() method.
   */
  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new LongsSketchBuildAggregatorFactory(getName(),
        getFieldName(), getMaxMapSize(), getErrorType(), getThreshold()));
  }
}
