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
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.LongsSketch;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.DirectReversePurgeLongHashMap;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@RunWith(Parameterized.class)
public class LongsSketchWrapTest
{
  private List<Long> dims;
  private int maxMapSize;
  private ErrorType errorType;

  public LongsSketchWrapTest(int maxMapSize, ErrorType errorType) throws IOException
  {
    this.maxMapSize = maxMapSize;
    this.errorType = errorType;

    dims = new ArrayList<>();
    File inputFile = new File(this.getClass().getClassLoader().getResource("frequencies/frequencies_longs_raw.tsv").getFile());
    InputStream inputStream = new FileInputStream(inputFile);
    LineIterator iter = IOUtils.lineIterator(inputStream, "UTF-8");
    while (iter.hasNext()) {
      String row = iter.next();
      dims.add(Long.valueOf(row.split("\t")[1]));
    }
    Collections.shuffle(dims);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters()
  {
    int[] maxMapSizes = {8, 16, 32, 64, 128, 256, 1024, 2048};
    ErrorType[] errorTypes = {ErrorType.NO_FALSE_POSITIVES, ErrorType.NO_FALSE_NEGATIVES};
    List<Object[]> params = new ArrayList<>();
    for (int m : maxMapSizes) {
      for (ErrorType e : errorTypes) {
        params.add(new Object[]{m, e});
      }
    }

    return params;
  }

  @Test
  public void testSerDe()
  {
    // Original LongsSketch
    LongsSketch longsSketch = new LongsSketch(maxMapSize);
    for (long dim : dims) {
      longsSketch.update(dim);
    }
    Memory expectedSerializedBytes = Memory.wrap(longsSketch.toByteArray());
    List<Pair<Long, Long>> expectKvs = extractKvsAndSort(expectedSerializedBytes);

    // LongsSketchWrap non direct
    LongsSketchWrap nonDirect = new LongsSketchWrap(maxMapSize);
    for (long dim : dims) {
      nonDirect.update(dim);
    }
    List<Pair<Long, Long>> actualKvs = extractKvsAndSort(Memory.wrap(nonDirect.toByteArray()));
    Assert.assertEquals(expectKvs, actualKvs);

    // LongsSketchWrap direct writable
    int bytesNeeded = LongsSketchWrap.getMaxStorageBytes(maxMapSize);
    final byte[] bytes = new byte[bytesNeeded];
    WritableMemory wmem = WritableMemory.wrap(bytes);
    LongsSketchWrap directWritable = new LongsSketchWrap(maxMapSize, wmem);
    for (long dim : dims) {
      directWritable.update(dim);
    }
    actualKvs = extractKvsAndSort(Memory.wrap(directWritable.toByteArray()));
    Assert.assertEquals(expectKvs, actualKvs);

    // LongsSketchWrap direct read-only
    LongsSketchWrap directReadOnly = new LongsSketchWrap(expectedSerializedBytes);
    actualKvs = extractKvsAndSort(Memory.wrap(directReadOnly.toByteArray()));
    Assert.assertEquals(expectKvs, actualKvs);

    // Deser empty LongsSketch
    LongsSketch longsSketchEmpty = new LongsSketch(maxMapSize);
    Memory serializedBytesEmpty = Memory.wrap(longsSketchEmpty.toByteArray());
    LongsSketchWrap directReadOnlyEmpty = new LongsSketchWrap(serializedBytesEmpty);
    Assert.assertEquals(0, directReadOnlyEmpty.getNumActiveItems());
  }

  /**
   * Make sure results don't change for sketches with different backing storages
   */
  @Test
  public void testEstimate()
  {
    // Original LongsSketch
    LongsSketch longsSketch = new LongsSketch(maxMapSize);
    for (long dim : dims) {
      longsSketch.update(dim);
    }
    Memory expectedSerializedBytes = Memory.wrap(longsSketch.toByteArray());
    List<String> expectedEstimation =
        Arrays.stream(longsSketch.getFrequentItems(errorType))
            .map(LongsSketch.Row::toString)
            .sorted()
            .collect(Collectors.toList());

    // LongsSketchWrap non direct
    LongsSketchWrap nonDirect = new LongsSketchWrap(maxMapSize);
    for (long dim : dims) {
      nonDirect.update(dim);
    }
    List<String> actualEstimation =
        Arrays.stream(nonDirect.getFrequentItems(errorType))
            .map(LongsSketchWrap.Row::toString)
            .sorted()
            .collect(Collectors.toList());
    Assert.assertEquals(expectedEstimation, actualEstimation);

    // LongsSketchWrap direct writable
    int bytesNeeded = LongsSketchWrap.getMaxStorageBytes(maxMapSize);
    final byte[] bytes = new byte[bytesNeeded];
    WritableMemory wmem = WritableMemory.wrap(bytes);
    LongsSketchWrap directWritable = new LongsSketchWrap(maxMapSize, wmem);
    for (long dim : dims) {
      directWritable.update(dim);
    }
    actualEstimation =
        Arrays.stream(directWritable.getFrequentItems(errorType))
            .map(LongsSketchWrap.Row::toString)
            .sorted()
            .collect(Collectors.toList());
    Assert.assertEquals(expectedEstimation, actualEstimation);

    // LongsSketchWrap direct read-only
    LongsSketchWrap directReadOnly = new LongsSketchWrap(expectedSerializedBytes);
    actualEstimation =
        Arrays.stream(directReadOnly.getFrequentItems(errorType))
            .map(LongsSketchWrap.Row::toString)
            .sorted()
            .collect(Collectors.toList());
    Assert.assertEquals(expectedEstimation, actualEstimation);
  }

  private List<Pair<Long, Long>> extractKvsAndSort(Memory mem)
  {
    List<Pair<Long, Long>> kvs = new ArrayList<>();
    if (mem.getCapacity() <= Long.BYTES) {
      return kvs;
    }

    int numActive = mem.getInt(Long.BYTES);
    int valuesOffset = Family.FREQUENCY.getMaxPreLongs() * Long.BYTES;
    int keysOffset = valuesOffset + numActive * DirectReversePurgeLongHashMap.VALUE_SIZE;
    for (int i = 0; i < numActive; i++) {
      kvs.add(Pair.of(
          mem.getLong(keysOffset + i * DirectReversePurgeLongHashMap.KEY_SIZE),
          mem.getLong(valuesOffset + i * DirectReversePurgeLongHashMap.VALUE_SIZE)));
    }
    Collections.sort(kvs);

    return kvs;
  }
}
