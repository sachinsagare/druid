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

package org.apache.druid.server.shard;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.ServerTestHelper;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.apache.druid.timeline.partition.SingleDimensionEvenSizeShardSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleDimensionEvenSizeShardSpecTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final List<SingleDimensionEvenSizeShardSpec> SHARD_SPECS = Arrays.asList(
      new SingleDimensionEvenSizeShardSpec("domain", "a.example.com", "c.example.com", 0, 5, 100000, 3000, 1000),
      new SingleDimensionEvenSizeShardSpec("domain", "c.example.com", "d.example.com", 1, 5, 100000, 2000, 1000),
      new SingleDimensionEvenSizeShardSpec("domain", "d.example.com", "d.example.com", 2, 5, 100000, 100000, 100000),
      new SingleDimensionEvenSizeShardSpec("domain", "d.example.com", "e.example.com", 3, 5, 100000, 4000, 1000),
      new SingleDimensionEvenSizeShardSpec("domain", "f.example.com", "g.example.com", 4, 5, 8000, 3000, 5000)
  );

  @Test
  public void testSerdeRoundTrip() throws Exception
  {
    testSerdeHelper(
        new SingleDimensionEvenSizeShardSpec(
            "country",
            "cn",
            "us",
            3,
            20,
            50,
            12,
            34
        ),
        "{\"type\":\"single_even_size\",\"dimension\":\"country\",\"start\":\"cn\",\"end\":\"us\",\"partitionNum\":3,\"partitions\":20,\"partitionSize\":50,\"startCount\":12,\"endCount\":34}"
    );
  }

  private void testSerdeHelper(SingleDimensionEvenSizeShardSpec spec, String expectedSerializedString)
      throws IOException
  {
    // Verify serialized format
    Assert.assertEquals(expectedSerializedString, ServerTestHelper.MAPPER.writeValueAsString(spec));
    // Verify round trip
    final ShardSpec deserializedSpec = ServerTestHelper.MAPPER.readValue(
        ServerTestHelper.MAPPER.writeValueAsBytes(spec),
        ShardSpec.class
    );

    Assert.assertEquals(spec, deserializedSpec);
  }

  @Test
  public void testGetLookup()
  {
    verifyDistribution("a.example.com", new int[]{0}, 1e-8);
    verifyDistribution("b.example.com", new int[]{0}, 1e-8);
    verifyDistribution("c.example.com", new int[]{0, 1}, 0.1);
    verifyDistribution("d.example.com", new int[]{1, 2, 3}, 0.1);
    verifyDistribution("e.example.com", new int[]{3}, 1e-8);
    verifyDistribution("f.example.com", new int[]{4}, 1e-8);
    verifyDistribution("g.example.com", new int[]{4}, 1e-8);

    expectedException.expect(ISE.class);
    expectedException.expectMessage("The number of candidate shard specs is 0");
    verifyDistribution("h.example.com", new int[]{4}, 1e-8);
  }

  private void verifyDistribution(
      String domain,
      int[] expectedShardSpecIndicesForDomain,
      double errorRate
  )
  {
    ShardSpecLookup lookup = SHARD_SPECS.get(0).getLookup(
        SHARD_SPECS.stream()
                   .map(s -> (ShardSpec) s)
                   .collect(Collectors.toList())
    );

    // Get expected
    Map<Integer, Integer> partitionNumToExpectedRowsInShard = new HashMap<>();
    List<SingleDimensionEvenSizeShardSpec> expectedShardSpecsForDomain = Arrays.stream(expectedShardSpecIndicesForDomain)
                                                                               .mapToObj(SHARD_SPECS::get)
                                                                               .collect(Collectors.toList());
    expectedShardSpecsForDomain
        .forEach(
            s -> {
              int rowsInShard;
              if (s.equals(expectedShardSpecsForDomain.get(0))) {
                rowsInShard = s.getEndCount();
              } else if (s.equals(expectedShardSpecsForDomain.get(expectedShardSpecsForDomain.size() - 1))) {
                rowsInShard = s.getStartCount();
              } else {
                rowsInShard = s.getPartitionSize();
              }
              partitionNumToExpectedRowsInShard.put(s.getPartitionNum(), rowsInShard);
            });
    int totalRows = partitionNumToExpectedRowsInShard.values().stream().mapToInt(v -> v).sum();

    // Get actual
    Map<Integer, Integer> partitionNumToActualRowsInShard = new HashMap<>();
    for (int i = 0; i < totalRows; i++) {
      SingleDimensionEvenSizeShardSpec actualShardSpecChosen = (SingleDimensionEvenSizeShardSpec) lookup.getShardSpec(
          123,
          new MapBasedInputRow(
              1,
              Arrays.asList("domain", "id"),
              ImmutableMap.of(
                  "domain", domain,
                  "id", "1"
              )
          )
      );
      Assert.assertTrue(expectedShardSpecsForDomain.stream().anyMatch(s -> s.equals(actualShardSpecChosen)));
      partitionNumToActualRowsInShard.put(
          actualShardSpecChosen.getPartitionNum(),
          partitionNumToActualRowsInShard.getOrDefault(
              actualShardSpecChosen.getPartitionNum(),
              0
          ) + 1
      );
    }

    // Check error rate
    Assert.assertEquals(partitionNumToExpectedRowsInShard.size(), partitionNumToActualRowsInShard.size());
    partitionNumToExpectedRowsInShard.forEach(
        (k, v) -> Assert.assertTrue(Math.abs((double) partitionNumToActualRowsInShard.get(k) - v) / v < errorRate)
    );
  }

  @Test
  public void testPossibleInDomain()
  {
    // Singleton value for the partition dimension
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("a.example.com")))),
        new int[]{0}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("b.example.com")))),
        new int[]{0}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("c.example.com")))),
        new int[]{0, 1}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("d.example.com")))),
        new int[]{1, 2, 3}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("e.example.com")))),
        new int[]{3}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("f.example.com")))),
        new int[]{4}
    );
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("g.example.com")))),
        new int[]{4}
    );
    // Singleton value not match for the partition dimension
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.singleton("h.example.com")))),
        new int[]{}
    );

    // Range value for the partition dimension
    verifyPossibleInDomain(
        ImmutableMap.of("domain", rangeSet(ImmutableList.of(Range.atMost("d.example.com")))),
        new int[]{0, 1, 2, 3}
    );
    // More than one value for the partition dimension
    verifyPossibleInDomain(
        ImmutableMap.of(
            "domain",
            rangeSet(ImmutableList.of(Range.atMost("c.example.com"), Range.singleton("g.example.com")))
        ), new int[]{0, 1, 4});
    // Wrong dimension so unable to prune, return everything
    verifyPossibleInDomain(
        ImmutableMap.of("host", rangeSet(ImmutableList.of(Range.lessThan("d.example.com")))),
        new int[]{0, 1, 2, 3, 4}
    );
    // More than one dimension but includes partition dimension, able to prune now
    verifyPossibleInDomain(ImmutableMap.of(
        "domain",
        rangeSet(ImmutableList.of(Range.singleton("a.example.com"))),
        "host",
        rangeSet(ImmutableList.of(Range.lessThan("d.example.com")))
    ), new int[]{0});
  }

  private void verifyPossibleInDomain(
      Map<String, RangeSet<String>> domain,
      int[] expectedShardSpecIndices
  )
  {
    List<SingleDimensionEvenSizeShardSpec> expectedShardSpecs = Arrays.stream(expectedShardSpecIndices)
                                                                      .mapToObj(SHARD_SPECS::get)
                                                                      .collect(Collectors.toList());
    List<SingleDimensionEvenSizeShardSpec> actual = SHARD_SPECS.stream()
                                                               .filter(s -> s.possibleInDomain(domain))
                                                               .collect(Collectors.toList());
    Assert.assertEquals(expectedShardSpecs.size(), actual.size());
    for (int i = 0; i < expectedShardSpecs.size(); i++) {
      Assert.assertEquals(expectedShardSpecs.get(i), actual.get(i));
    }
  }

  private static RangeSet<String> rangeSet(List<Range<String>> ranges)
  {
    ImmutableRangeSet.Builder<String> builder = ImmutableRangeSet.builder();
    for (Range<String> range : ranges) {
      builder.add(range);
    }
    return builder.build();
  }
}
