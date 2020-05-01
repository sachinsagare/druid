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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;


/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class LongsSketchBuildAggregator implements Aggregator
{
  private final ColumnValueSelector<Object> selector;
  private LongsSketchWrap sketch;
  private int maxMapSize;

  public LongsSketchBuildAggregator(
      final ColumnValueSelector<Object> selector,
      final int maxMapSize
  )
  {
    this.selector = selector;
    this.sketch = new LongsSketchWrap(maxMapSize);
    this.maxMapSize = maxMapSize;
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }
    synchronized (this) {
      updateSketch(sketch, value);
    }
  }

  /*
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public synchronized Object get()
  {
    // Return a copy
    LongsSketchWrap ret = new LongsSketchWrap(maxMapSize);
    ret.merge(sketch);
    return ret;
  }

  @Override
  public void close()
  {
    sketch = null;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  static void updateSketch(final LongsSketchWrap sketch, final Object value)
  {
    if (value instanceof Integer || value instanceof Long) {
      sketch.update(((Number) value).longValue());
    } else if (value instanceof int[]) {
      for (int v : (int[]) value) {
        sketch.update(v);
      }
    } else if (value instanceof long[]) {
      for (long v : (long[]) value) {
        sketch.update(v);
      }
    } else if (value instanceof String) {
      try {
        sketch.update(Long.parseLong((String) value));
      }
      catch (NumberFormatException e) {
        throw new IAE("Value can't be parse to long type " + value);
      }
    } else if (value instanceof List) {
      List<Long> values = (List<Long>) value;
      try {
        for (Long v : values) {
          sketch.update(v);
        }
      }
      catch (ClassCastException e) {
        throw new IAE("Value can't be cast to List<Long> type " + value);
      }
    } else {
      throw new IAE("Unsupported type " + value.getClass());
    }
  }

}
