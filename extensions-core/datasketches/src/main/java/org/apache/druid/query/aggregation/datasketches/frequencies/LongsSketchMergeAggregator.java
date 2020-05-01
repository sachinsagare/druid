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

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.ColumnValueSelector;


/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link LongsSketchWrap}
 */
public class LongsSketchMergeAggregator implements Aggregator
{
  private final ColumnValueSelector<LongsSketchWrap> selector;
  private LongsSketchWrap union;

  public LongsSketchMergeAggregator(
      final ColumnValueSelector<LongsSketchWrap> selector,
      final int maxMapSize
  )
  {
    this.selector = selector;
    this.union = new LongsSketchWrap(maxMapSize);
  }

  /**
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final LongsSketchWrap sketch = selector.getObject();
    if (sketch == null) {
      return;
    }

    synchronized (this) {
      if (union.getMaxMapSize() < sketch.getMaxMapSize()) {
        LongsSketchWrap newUnion = new LongsSketchWrap(sketch.getMaxMapSize());
        union = newUnion.merge(union);
      }
      union.merge(sketch);
    }
  }

  /**
   * This method is synchronized because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently.
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public synchronized Object get()
  {
    return union;
  }

  @Override
  public void close()
  {
    union = null;
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
}
