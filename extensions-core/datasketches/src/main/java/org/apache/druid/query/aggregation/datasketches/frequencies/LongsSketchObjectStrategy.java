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

import com.yahoo.memory.Memory;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongsSketchObjectStrategy implements ObjectStrategy<LongsSketchWrap>
{
  static final LongsSketchObjectStrategy STRATEGY = new LongsSketchObjectStrategy();

  @Override
  public Class<LongsSketchWrap> getClazz()
  {
    return LongsSketchWrap.class;
  }

  @Override
  public int compare(final LongsSketchWrap sketch1, final LongsSketchWrap sketch2)
  {
    return LongsSketchAggregatorFactory.COMPARATOR.compare(sketch1, sketch2);
  }

  @Override
  public LongsSketchWrap fromByteBuffer(final ByteBuffer buf, final int size)
  {
    return new LongsSketchWrap(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN).region(buf.position(), size));
  }

  @Override
  public byte[] toBytes(final LongsSketchWrap sketch)
  {
    return sketch.toByteArray();
  }
}
