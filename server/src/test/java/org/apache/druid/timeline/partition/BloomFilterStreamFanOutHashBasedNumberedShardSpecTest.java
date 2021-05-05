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

package org.apache.druid.timeline.partition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.server.ServerTestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BloomFilterStreamFanOutHashBasedNumberedShardSpecTest
{
  @Test
  public void testSerdeRoundTrip() throws Exception
  {
    // Test empty bloom filter
    BloomFilterStreamFanOutHashBasedNumberedShardSpec spec = new BloomFilterStreamFanOutHashBasedNumberedShardSpec(
        1,
        2,
        ImmutableList.of("dim1"),
        ImmutableSet.of(1, 3, 5),
        10,
        5,
        ServerTestHelper.MAPPER
    );
    testSerdeHelper(
        spec,
        "{\"type\":\"bloom_filter_stream_fanout_hashed\",\"partitionNum\":1,\"partitions\":2,\"partitionDimensions\":[\"dim1\"],\"streamPartitionIds\":[1,3,5],\"streamPartitions\":10,\"fanOutSize\":5}"
    );

    // Test non empty bloom filter
    spec.updateBloomFilter(
        new MapBasedInputRow(
            1L,
            ImmutableList.of("dim1", "dim2"),
            ImmutableMap.of("dim1", "abc", "dim2", "foo")
        )
    );
    spec.completeBloomFilter();
    testSerdeHelper(
        spec,
        "{\"type\":\"bloom_filter_stream_fanout_hashed\",\"partitionNum\":1,\"partitions\":2,\"partitionDimensions\":[\"dim1\"],\"streamPartitionIds\":[1,3,5],\"streamPartitions\":10,\"fanOutSize\":5}"
    );
  }

  void testSerdeHelper(BloomFilterStreamFanOutHashBasedNumberedShardSpec spec, String expectedSerializedString) throws IOException
  {
    // Verify serialized format
    Assert.assertEquals(expectedSerializedString, ServerTestHelper.MAPPER.writeValueAsString(spec));
    // Verify round trip
    final BloomFilterStreamFanOutHashBasedNumberedShardSpec deserializedSpec =
        (BloomFilterStreamFanOutHashBasedNumberedShardSpec) ServerTestHelper.MAPPER.readValue(
            ServerTestHelper.MAPPER.writeValueAsBytes(spec),
            ShardSpec.class
        );
    Assert.assertEquals(spec.getPartitionNum(), deserializedSpec.getPartitionNum());
    Assert.assertEquals(spec.getPartitions(), deserializedSpec.getPartitions());
    Assert.assertEquals(
        spec.getPartitionDimensions(),
        deserializedSpec.getPartitionDimensions()
    );
    Assert.assertEquals(
        spec.getStreamPartitionIds(),
        deserializedSpec.getStreamPartitionIds()
    );
    Assert.assertEquals(
        spec.getStreamPartitions(),
        deserializedSpec.getStreamPartitions()
    );
    Assert.assertEquals(
        spec.getFanOutSize(),
        deserializedSpec.getFanOutSize()
    );

    byte[] bloomFilter = spec.serializeBloomFilter();
    deserializedSpec.deserializeBloomFilter(BloomFilterStreamFanOutHashBasedNumberedShardSpec.V1_VERSION, bloomFilter);
    Assert.assertEquals(spec.getBloomFilter(), deserializedSpec.getBloomFilter());
  }

  @Test
  public void testCompatible()
  {
    final BloomFilterStreamFanOutHashBasedNumberedShardSpec spec = new BloomFilterStreamFanOutHashBasedNumberedShardSpec(
        1,
        2,
        ImmutableList.of("dim1"),
        ImmutableSet.of(1, 3, 5),
        10,
        null,
        ServerTestHelper.MAPPER
    );

    Assert.assertTrue(spec.isCompatible(NumberedShardSpec.class));
    Assert.assertTrue(spec.isCompatible(NumberedOverwriteShardSpec.class));
    Assert.assertTrue(spec.isCompatible(StreamHashBasedNumberedShardSpec.class));
    Assert.assertTrue(spec.isCompatible(StreamFanOutHashBasedNumberedShardSpec.class));
    Assert.assertTrue(spec.isCompatible(BloomFilterStreamFanOutHashBasedNumberedShardSpec.class));
  }

  @Test
  public void testPossibleInDomain()
  {
    final int streamPartitions = 10;
    List<BloomFilterStreamFanOutHashBasedNumberedShardSpec> shardSpecs = ImmutableList.of(
        new BloomFilterStreamFanOutHashBasedNumberedShardSpec(
            1,
            0,
            ImmutableList.of("dim1"),
            ImmutableSet.of(1, 3, 5),
            streamPartitions,
            null,
            ServerTestHelper.MAPPER
        ),
        new BloomFilterStreamFanOutHashBasedNumberedShardSpec(
            2,
            0,
            ImmutableList.of("dim1"),
            ImmutableSet.of(2, 4, 6),
            streamPartitions,
            null,
            ServerTestHelper.MAPPER
        ),
        new BloomFilterStreamFanOutHashBasedNumberedShardSpec(
            3,
            0,
            ImmutableList.of("dim1"),
            ImmutableSet.of(7, 8, 9, 10),
            streamPartitions,
            null,
            ServerTestHelper.MAPPER
        )
    );

    final RangeSet<String> rangeSet1 = TreeRangeSet.create();
    rangeSet1.add(Range.closed("123", "123"));
    final Map<String, RangeSet<String>> domain1 = ImmutableMap.of("dim1", rangeSet1);

    final RangeSet<String> rangeSet2 = TreeRangeSet.create();
    rangeSet2.add(Range.closed("456", "456"));
    final Map<String, RangeSet<String>> domain2 = ImmutableMap.of("dim1", rangeSet2);

    // Partition dimensions match: before completing bloom filter, rely on hashing alone
    Assert.assertEquals(1, shardSpecs.stream().filter(s -> s.possibleInDomain(domain1)).count());
    Assert.assertEquals(1, shardSpecs.stream().filter(s -> s.possibleInDomain(domain2)).count());

    // Partition dimensions match: after completing bloom filter, rely on both hashing and bloom filter
    shardSpecs.stream()
              .filter(s -> s.possibleInDomain(domain1))
              .forEach(s -> s.updateBloomFilter(new MapBasedInputRow(
                  1L,
                  ImmutableList.of("dim1", "dim2"),
                  ImmutableMap.of("dim1", "123", "dim2", "foo")
              )));
    shardSpecs.forEach(BloomFilterStreamFanOutHashBasedNumberedShardSpec::completeBloomFilter);
    Assert.assertEquals(1, shardSpecs.stream().filter(s -> s.possibleInDomain(domain1)).count());
    Assert.assertEquals(0, shardSpecs.stream().filter(s -> s.possibleInDomain(domain2)).count());

    // Partition dimensions not match
    final Map<String, RangeSet<String>> domain3 = ImmutableMap.of("vistor_id", rangeSet1);
    Assert.assertEquals(shardSpecs.size(), shardSpecs.stream().filter(s -> s.possibleInDomain(domain3)).count());
  }
}
