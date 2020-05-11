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

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer
    implements GenericColumnSerializer<LongsSketchWrap>
{
  public static LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    return new LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer(
        segmentWriteOutMedium,
        filenameBase,
        strategy
    );
  }

  public static final byte VERSION_BLOCK_COMPRESSED = 0x3;
  private static final int METADATA_NUM_SKETCHES_IN_BLOCK = Integer.BYTES;
  private static final MetaSerdeHelper<LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer> META_SERDE_HELPER =
      // Write version
      MetaSerdeHelper.firstWriteByte(
          (LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer x) -> VERSION_BLOCK_COMPRESSED)
                     // Write number of blocks
                     .writeInt(x -> Ints.checkedCast(x.sketchBlockIndex.size()))
                     // Write sketch block index
                     .writeByteArray(x -> x.sketchBlockIndexWriteBuffer.array());

  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final ObjectStrategy strategy;
  // The temp buffer (64 KB) to write result to before feeding into a compressor
  private static final int DEFAULT_COMPRESSION_BUFFER_SIZE = 0x10000;
  // Record one per block
  private final IntList sketchBlockIndex = new IntArrayList();
  private final ByteBuffer sketchWithByteOffsetWriteBuffer = ByteBuffer.allocate(DEFAULT_COMPRESSION_BUFFER_SIZE).order(
      IndexIO.BYTE_ORDER);
  private final IntList sketchByteOffsetsInBlock = new IntArrayList();
  private int sketchIndex = -1;
  private ByteBuffer sketchBlockIndexWriteBuffer;
  private final List<byte[]> sketchesInBlock = new ArrayList<>();
  private int sketchCumulativeSizeInBlock = 0;

  private LargeColumnSupportedLongsSketchColumnBlockCompressedSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy
  )
  {
    this.strategy = strategy;
    this.flattener = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        filenameBase,
        CompressionStrategy.LZ4,
        DEFAULT_COMPRESSION_BUFFER_SIZE
    );
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends LongsSketchWrap> selector) throws IOException
  {
    byte[] sketchBytes = strategy.toBytes(selector.getObject());

    int singleSketchBytesSize = Ints.checkedCast((long) METADATA_NUM_SKETCHES_IN_BLOCK
                                                 + Integer.BYTES
                                                 + sketchBytes.length);
    if (singleSketchBytesSize > DEFAULT_COMPRESSION_BUFFER_SIZE) {
      throw new ISE("Single sketch bytes to write greater than compression buffer size");
    } else if (singleSketchBytesSize + sketchCumulativeSizeInBlock > DEFAULT_COMPRESSION_BUFFER_SIZE) {
      flush();
    }

    sketchCumulativeSizeInBlock = Ints.checkedCast((long) sketchCumulativeSizeInBlock
                                                   + Integer.BYTES
                                                   + sketchBytes.length);
    int sketchByteOffsetInBlock = Ints.checkedCast((long) sketchBytes.length +
                                                   (sketchByteOffsetsInBlock.isEmpty()
                                                    ? 0
                                                    : sketchByteOffsetsInBlock.getInt(sketchByteOffsetsInBlock.size()
                                                                                      - 1)));
    sketchByteOffsetsInBlock.add(sketchByteOffsetInBlock);
    sketchesInBlock.add(sketchBytes);
    sketchIndex++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    wrapUp();
    return META_SERDE_HELPER.size(this) + flattener.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    wrapUp();
    META_SERDE_HELPER.writeTo(channel, this);
    flattener.writeTo(channel, smoosher);
  }

  private void flush() throws IOException
  {
    if (sketchesInBlock.size() == 0) {
      return;
    }
    // Write num of sketches in the block
    sketchWithByteOffsetWriteBuffer.putInt(sketchesInBlock.size());
    // Write sketch byte offsets
    for (int e : sketchByteOffsetsInBlock) {
      sketchWithByteOffsetWriteBuffer.putInt(e);
    }
    // Write sketches
    for (byte[] e : sketchesInBlock) {
      sketchWithByteOffsetWriteBuffer.put(e);
    }

    sketchWithByteOffsetWriteBuffer.flip();
    flattener.write(sketchWithByteOffsetWriteBuffer);
    sketchBlockIndex.add(sketchIndex);

    sketchCumulativeSizeInBlock = 0;
    sketchWithByteOffsetWriteBuffer.clear();
    sketchByteOffsetsInBlock.clear();
    sketchesInBlock.clear();
  }

  private void wrapUp() throws IOException
  {
    flush();
    if (sketchBlockIndexWriteBuffer == null) {
      int numBlocks = sketchBlockIndex.size();
      sketchBlockIndexWriteBuffer = ByteBuffer.allocate(Integer.BYTES * numBlocks);
      for (int e : sketchBlockIndex) {
        sketchBlockIndexWriteBuffer.putInt(e);
      }
    }
  }
}
