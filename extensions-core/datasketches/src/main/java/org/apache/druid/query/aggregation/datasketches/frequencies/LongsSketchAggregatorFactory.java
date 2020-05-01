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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;


/**
 * Base class for both build and merge factories
 */
public abstract class LongsSketchAggregatorFactory extends AggregatorFactory
{
  public static final int DEFAULT_MAX_MAP_SIZE = 8;
  public static final ErrorType DEFAULT_ERROR_TYPE = ErrorType.NO_FALSE_NEGATIVES;

  static final Comparator<LongsSketchWrap> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingInt(LongsSketchWrap::getNumActiveItems));

  private final String name;
  private final String fieldName;
  private final int maxMapSize;
  private final ErrorType errorType;
  private final long threshold;

  LongsSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer maxMapSize,
      @Nullable final String errorType,
      @Nullable final Long threshold
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.maxMapSize = maxMapSize == null ? DEFAULT_MAX_MAP_SIZE : maxMapSize;
    this.errorType = errorType == null ? DEFAULT_ERROR_TYPE : ErrorType.valueOf(errorType);
    this.threshold = threshold == null ? LongsSketchWrap.DEFAULT_THRESHOLD : threshold;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getMaxMapSize()
  {
    return maxMapSize;
  }

  @JsonProperty
  public String getErrorType()
  {
    return errorType.toString();
  }

  @JsonProperty
  public long getThreshold()
  {
    return threshold;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  /**
   * This is a convoluted way to return a list of input field names this aggregator needs.
   * Currently the returned factories are only used to obtain a field name by calling getName() method.
   */
  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new LongsSketchMergeAggregatorFactory(getName(),
        getFieldName(), getMaxMapSize(), getErrorType(), getThreshold()));
  }

  @Override
  public LongsSketchWrap deserialize(final Object object)
  {
    return LongsSketchMergeComplexMetricSerde.deserializeSketch(object);
  }

  @Override
  public LongsSketchWrap combine(final Object objectA, final Object objectB)
  {
    LongsSketchWrap sketchA = (LongsSketchWrap) objectA;
    LongsSketchWrap sketchB = (LongsSketchWrap) objectB;
    int maxMapSize = Math.max(sketchA.getMaxMapSize(), sketchB.getMaxMapSize());
    final LongsSketchWrap union = new LongsSketchWrap(maxMapSize);
    union.merge(sketchA);
    union.merge(sketchB);
    return union;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<LongsSketchWrap>()
    {
      private LongsSketchWrap union = new LongsSketchWrap(DEFAULT_MAX_MAP_SIZE);

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        union.reset();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        final LongsSketchWrap sketch = (LongsSketchWrap) selector.getObject();
        if (union.getMaxMapSize() < sketch.getMaxMapSize()) {
          LongsSketchWrap newUnion = new LongsSketchWrap(sketch.getMaxMapSize());
          union = newUnion.merge(union);
        }
        union.merge(sketch);
      }

      @Nullable
      @Override
      public LongsSketchWrap getObject()
      {
        return union;
      }

      @Override
      public Class<LongsSketchWrap> classOfObject()
      {
        return LongsSketchWrap.class;
      }
    };
  }

  @Override
  public Object finalizeComputation(final Object object)
  {
    StringBuilder ret = new StringBuilder();
    ret.append('[');
    if (object != null) {
      final LongsSketchWrap sketch = (LongsSketchWrap) object;
      LongsSketchWrap.Row[] items = this.threshold != LongsSketchWrap.DEFAULT_THRESHOLD ?
          sketch.getFrequentItems(this.threshold, this.errorType) :
          sketch.getFrequentItems(this.errorType);
      for (LongsSketchWrap.Row row : items) {
        if (ret.length() > 1) {
          ret.append(',');
        }
        ret.append(row.getJson());
      }
    }
    ret.append(']');

    return ret.toString();
  }

  @Override
  public Comparator<LongsSketchWrap> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongsSketchMergeAggregatorFactory(getName(), getName(), getMaxMapSize(),
        getErrorType(), getThreshold());
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(getCacheTypeId()).appendString(name).appendString(fieldName)
        .appendInt(maxMapSize).appendInt(errorType.ordinal()).appendInt((int) threshold).build();
  }

  @Override
  public boolean equals(final Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().equals(object.getClass())) {
      return false;
    }
    final LongsSketchAggregatorFactory that = (LongsSketchAggregatorFactory) object;
    if (!name.equals(that.getName())) {
      return false;
    }
    if (!fieldName.equals(that.getFieldName())) {
      return false;
    }
    if (maxMapSize != that.getMaxMapSize()) {
      return false;
    }
    if (!errorType.toString().equals(that.getErrorType())) {
      return false;
    }
    if (threshold != that.getThreshold()) {
      return false;
    }
    return true;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return LongsSketchWrap.getMaxStorageBytes(maxMapSize);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, maxMapSize, errorType, threshold);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + " {"
        + "name=" + name
        + "fieldName=" + fieldName
        + "maxMapSize=" + maxMapSize
        + "errorType=" + errorType.toString()
        + "threshold=" + threshold
        + "}";
  }

  protected abstract byte getCacheTypeId();

}
