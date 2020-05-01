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
import com.yahoo.sketches.frequencies.LongsSketch;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 * This aggregator factory is for merging existing sketches.
 * The input column must contain {@link LongsSketch}
 */
public class LongsSketchMergeAggregatorFactory extends LongsSketchAggregatorFactory
{
  @JsonCreator
  public LongsSketchMergeAggregatorFactory(
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
    return LongsSketchModule.MERGE_TYPE_NAME;
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.LONGS_SKETCH_MERGE_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<LongsSketchWrap> selector =
        columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new LongsSketchMergeAggregator(selector, getMaxMapSize());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<LongsSketchWrap> selector =
        columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new LongsSketchMergeBufferAggregator(
        selector,
        getMaxMapSize(),
        getMaxIntermediateSize()
    );
  }
}
