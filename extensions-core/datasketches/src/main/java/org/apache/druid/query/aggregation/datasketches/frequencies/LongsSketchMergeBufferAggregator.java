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

import com.google.common.util.concurrent.Striped;
import com.yahoo.memory.WritableMemory;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;


/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link LongsSketchWrap}
 */
public class LongsSketchMergeBufferAggregator implements BufferAggregator
{
  /** for locking per buffer position (power of 2 to make index computation faster) */
  private static final int NUM_STRIPES = 64;

  private final ColumnValueSelector<LongsSketchWrap> selector;
  private final int maxMapSize;
  private final int size;
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<LongsSketchWrap>> sketchCache = new IdentityHashMap<>();

  public LongsSketchMergeBufferAggregator(
      final ColumnValueSelector<LongsSketchWrap> selector,
      final int maxMapSize,
      final int size
  )
  {
    this.selector = selector;
    this.maxMapSize = maxMapSize;
    this.size = size;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory wmem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN).writableRegion(position, size);
    LongsSketchBuildBufferAggregator.putSketchIntoCache(sketchCache, buf, position, new LongsSketchWrap(maxMapSize, wmem));
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final LongsSketchWrap sketch = selector.getObject();
    if (sketch == null) {
      return;
    }
    final Lock lock = stripedLock.getAt(LongsSketchBuildBufferAggregator.lockIndex(position)).writeLock();
    lock.lock();
    try {
      final LongsSketchWrap union = sketchCache.get(buf).get(position);
      union.merge(sketch);
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final Lock lock = stripedLock.getAt(LongsSketchBuildBufferAggregator.lockIndex(position)).readLock();
    lock.lock();
    try {
      // Do we need to return a deep on-heap copy here instead?
      return sketchCache.get(buf).get(position);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    // maxMapSize should be inspected because different execution paths exist in
    // LongsSketchWrap.update() that is called from @CalledFromHotLoop-annotated aggregate()
    // depending on some variable derived from maxMapSize
    inspector.visit("maxMapSize", maxMapSize);
  }
}
