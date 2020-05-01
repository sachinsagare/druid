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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class LargeColumnSupportedLongsSketchColumnCompressedSerializer implements GenericColumnSerializer<LongsSketchWrap>
{
  public static LargeColumnSupportedLongsSketchColumnCompressedSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy,
      CompressionStrategy compression
  )
  {
    return new LargeColumnSupportedLongsSketchColumnCompressedSerializer(segmentWriteOutMedium, filenameBase, strategy, compression);
  }

  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final ObjectStrategy strategy;
  // The temp buffer to write result to before feeding into a compressoor
  private static final int DEFAULT_COMPRESSION_BUFFER_SIZE = 0x10000;

  private LargeColumnSupportedLongsSketchColumnCompressedSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy strategy,
      CompressionStrategy compression
  )
  {
    this.strategy = strategy;
    this.flattener = GenericIndexedWriter.ofCompressedByteBuffers(segmentWriteOutMedium, filenameBase, compression, DEFAULT_COMPRESSION_BUFFER_SIZE);
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends LongsSketchWrap> selector) throws IOException
  {
    ByteBuffer bytesToWrite = ByteBuffer.wrap(strategy.toBytes(selector.getObject()));

    if (bytesToWrite.capacity() > DEFAULT_COMPRESSION_BUFFER_SIZE) {
      throw new ISE("sketch bytes to write greater than compression buffer size");
    }
    flattener.write(bytesToWrite);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return flattener.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    flattener.writeTo(channel, smoosher);
  }
}
