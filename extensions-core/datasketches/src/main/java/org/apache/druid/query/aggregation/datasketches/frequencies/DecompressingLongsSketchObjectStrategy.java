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

import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DecompressingByteBufferObjectStrategy;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecompressingLongsSketchObjectStrategy implements ObjectStrategy<LongsSketchWrap>
{
  private final DecompressingByteBufferObjectStrategy decompressingByteBufferObjectStrategy;

  public DecompressingLongsSketchObjectStrategy(ByteOrder order, CompressionStrategy compression)
  {
    this.decompressingByteBufferObjectStrategy =
        new DecompressingByteBufferObjectStrategy(order, compression);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<LongsSketchWrap> getClazz()
  {
    return LongsSketchWrap.class;
  }

  @Override
  public LongsSketchWrap fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    ByteBuffer decompressedBytes = decompressingByteBufferObjectStrategy.fromByteBuffer(buffer, numBytes).get();
    return LongsSketchObjectStrategy.STRATEGY.fromByteBuffer(decompressedBytes, decompressedBytes.capacity());
  }

  @Override
  public int compare(LongsSketchWrap o1, LongsSketchWrap o2)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] toBytes(LongsSketchWrap longsSketchWrap)
  {
    throw new UnsupportedOperationException();
  }
}
