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

package org.apache.druid.query.aggregation.datasketches.frequencies.indexio;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DecompressingByteBufferObjectStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class LongsSketchColumnIndexed
{
  private ObjectStrategy<LongsSketchWrap> objectStrategy;
  private GenericIndexed<ResourceHolder<ByteBuffer>> sketchBlocks;
  private final ByteBuffer buffer;
  // Inclusive
  private final int sketchBlockIndexBufferFrom;
  // Exclusive
  private final int sketchBlockIndexBufferTo;
  private int curBlockIndex = -1;
  private int curNumSketchesInBlock;
  private ByteBuffer curSketchBlockBuffer;

  public LongsSketchColumnIndexed(
      ByteBuffer buffer,
      ObjectStrategy<LongsSketchWrap> objectStrategy
  )
  {
    byte version = buffer.get();
    if (version == LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer.VERSION_BLOCK_COMPRESSED) {
      int numBlocks = buffer.getInt();
      sketchBlockIndexBufferFrom = buffer.position();
      sketchBlockIndexBufferTo = buffer.position() + Integer.BYTES * numBlocks;
      buffer.position(sketchBlockIndexBufferTo);
      sketchBlocks = GenericIndexed.read(
          buffer,
          new DecompressingByteBufferObjectStrategy(
              IndexIO.BYTE_ORDER,
              CompressionStrategy.LZ4
          )
      );

      this.buffer = buffer;
      this.objectStrategy = objectStrategy;
    } else {
      throw new ISE("Unknown version");
    }
  }

  public LongsSketchWrap get(int index)
  {
    Pair<Integer, Integer> sketchIndex = getSketchIndex(index);

    int blockIndex = sketchIndex.getLeft();
    if (curBlockIndex != blockIndex) {
      curSketchBlockBuffer = sketchBlocks.get(blockIndex).get().order(IndexIO.BYTE_ORDER);
      curNumSketchesInBlock = curSketchBlockBuffer.getInt();
      curBlockIndex = blockIndex;
    }
    ByteBuffer sketchBlockBuffer = curSketchBlockBuffer.asReadOnlyBuffer().order(IndexIO.BYTE_ORDER);

    int indexInBlock = sketchIndex.getRight();
    int startByteOffset;
    int endByteOffset;
    if (indexInBlock == 0) {
      startByteOffset = 0;
      endByteOffset = sketchBlockBuffer.getInt(sketchBlockBuffer.position());
    } else {
      int offset = (indexInBlock - 1) * Integer.BYTES;
      startByteOffset = sketchBlockBuffer.getInt(sketchBlockBuffer.position() + offset);
      endByteOffset = sketchBlockBuffer.getInt(sketchBlockBuffer.position() + offset + Integer.BYTES);
    }

    int size = endByteOffset - startByteOffset;
    sketchBlockBuffer.position(sketchBlockBuffer.position()
                               + curNumSketchesInBlock * Integer.BYTES
                               + startByteOffset);
    return objectStrategy.fromByteBuffer(sketchBlockBuffer, size);
  }

  public int size()
  {
    throw new UnsupportedOperationException("Size not implemented.");
  }

  public int indexOf(@Nullable LongsSketchWrap value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  /**
   * @return Pair.of(blockIndex, indexInBlock)
   */
  private Pair<Integer, Integer> getSketchIndex(int index)
  {
    int blockIndex = -1;
    int size = (sketchBlockIndexBufferTo - sketchBlockIndexBufferFrom) / Integer.BYTES;
    int low = 0;
    int high = size - 1;

    assert low <= high;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = buffer.getInt(sketchBlockIndexBufferFrom + mid * Integer.BYTES);

      if (midVal < index) {
        low = mid + 1;
      } else if (midVal > index) {
        high = mid - 1;
      } else {
        blockIndex = mid; // key found
        break;
      }
    }
    if (low > high) {
      // key not found.
      blockIndex = low;
    }

    assert blockIndex != -1;
    int indexInBlock = index - (blockIndex > 0 ? (1 + buffer.getInt(sketchBlockIndexBufferFrom
                                                                    + (blockIndex - 1) * Integer.BYTES)) : 0);
    return Pair.of(blockIndex, indexInBlock);
  }
}

