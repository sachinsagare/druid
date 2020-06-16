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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();
  private static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,    // partitionId
      @JsonProperty("partitions") int partitions,        // # of partitions
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    this.jsonMapper = jsonMapper;
    this.partitionDimensions = partitionDimensions == null ? DEFAULT_PARTITION_DIMENSIONS : partitionDimensions;
  }

  @JsonProperty("partitionDimensions")
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public boolean isCompatible(Class<? extends ShardSpec> other)
  {
    return other == HashBasedNumberedShardSpec.class;
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return (Math.abs(hash(timestamp, inputRow)) - getPartitionNum()) % getPartitions() == 0;
  }

  protected int hash(long timestamp, InputRow inputRow)
  {
    final List<Object> groupKey = getGroupKey(timestamp, inputRow);
    try {
      return hash(jsonMapper, groupKey);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  List<Object> getGroupKey(final long timestamp, final InputRow inputRow)
  {
    if (partitionDimensions.isEmpty()) {
      return Rows.toGroupKey(timestamp, inputRow);
    } else {
      return Lists.transform(partitionDimensions, inputRow::getDimension);
    }
  }

  @VisibleForTesting
  public static int hash(ObjectMapper jsonMapper, List<Object> objects) throws JsonProcessingException
  {
    return HASH_FUNCTION.hashBytes(jsonMapper.writeValueAsBytes(objects)).asInt();
  }

  @Override
  public String toString()
  {
    return "HashBasedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getPartitions() +
           ", partitionDimensions=" + getPartitionDimensions() +
           '}';
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> {
      int index = Math.abs(hash(timestamp, row) % getPartitions());
      return shardSpecs.get(index);
    };
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    // If no partitionDimensions are specified during ingestion, hash is based on all dimensions
    // plus the truncated input timestamp according to QueryGranularity instead of just
    // partitionDimensions. Since we don't record the timetamp here, bypass this case.
    if (partitionDimensions.isEmpty()) {
      return true;
    }

    // One possible optimization is to move the conversion from range set to point set to the
    // function signature and cache it in the caller of this function if there are repetive calls of
    // the same domain
    Map<String, Set<String>> domainPointSet = new HashMap<>();
    for (String p : partitionDimensions) {
      RangeSet<String> domainRangeSet = domain.get(p);
      if (domainRangeSet == null || domainRangeSet.isEmpty()) {
        return true;
      }

      for (Range<String> v : domainRangeSet.asRanges()) {
        // If there are range values, simply bypass, because we can't hash range values
        if (v.isEmpty() || !v.hasLowerBound() || !v.hasUpperBound() ||
            v.lowerBoundType() != BoundType.CLOSED || v.upperBoundType() != BoundType.CLOSED ||
            !v.lowerEndpoint().equals(v.upperEndpoint())) {
          return true;
        }
        domainPointSet.computeIfAbsent(p, k -> new HashSet<>()).add(v.lowerEndpoint());
      }
    }

    return !domainPointSet.isEmpty() && chunkPossibleInDomain(domainPointSet, new HashMap<>());
  }

  // Enumerate all possible combinations of partition dimensions from domain, return chunk not in
  // domain if and only if none of the combinations can be in chunk
  private boolean chunkPossibleInDomain(Map<String, Set<String>> domainPointSet,
                                        Map<String, String> partitionDimensionsFromDomain)
  {
    int curIndex = partitionDimensionsFromDomain.size();
    if (curIndex == partitionDimensions.size()) {
      return isInChunk(partitionDimensionsFromDomain);
    }

    String dimension = partitionDimensions.get(curIndex);
    for (String e : domainPointSet.get(dimension)) {
      partitionDimensionsFromDomain.put(dimension, e);
      if (chunkPossibleInDomain(domainPointSet, partitionDimensionsFromDomain)) {
        return true;
      }
      partitionDimensionsFromDomain.remove(dimension);
    }

    return false;
  }

  // Just another isInChunk with different signature
  private boolean isInChunk(Map<String, String> partitionDimensionsFromDomain)
  {
    assert !partitionDimensions.isEmpty();
    List<Object> groupKey = Lists.transform(partitionDimensions,
        o -> Collections.singletonList(partitionDimensionsFromDomain.get(o)));

    return groupKeyIsInChunk(groupKey);
  }

  protected boolean groupKeyIsInChunk(List<Object> groupKey)
  {
    try {
      return (Math.abs(hash(jsonMapper, groupKey)) - getPartitionNum()) % getPartitions() == 0;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
