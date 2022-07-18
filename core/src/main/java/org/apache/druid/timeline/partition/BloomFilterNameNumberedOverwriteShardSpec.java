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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BloomFilterNameNumberedOverwriteShardSpec extends NamedNumberedShardSpec implements OverwriteShardSpec
{

  @JsonIgnore
  private final String dimension;
  @JsonIgnore
  private final BloomFilter<Long> bloomFilter;
  @JsonIgnore
  private final List<String> domainDimensions;

  private final short startRootPartitionId;
  private final short endRootPartitionId; // exclusive
  private final short minorVersion;
  private final short atomicUpdateGroupSize; // number of segments in atomicUpdateGroup

  @JsonCreator
  public BloomFilterNameNumberedOverwriteShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionName") @Nullable String partitionName,
      @JsonProperty("startRootPartitionId") int startRootPartitionId,
      @JsonProperty("endRootPartitionId") int endRootPartitionId,
      @JsonProperty("minorVersion") short minorVersion,
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bloomFilter") String bloomFilter,
      @JsonProperty("atomicUpdateGroupSize") short atomicUpdateGroupSize
  )
  {
    super(partitionNum, partitions, partitionName);
    this.startRootPartitionId = (short) startRootPartitionId;
    this.endRootPartitionId = (short) endRootPartitionId;
    this.minorVersion = minorVersion;
    this.dimension = dimension;
    this.domainDimensions = ImmutableList.of(dimension);
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(StringUtils.decodeBase64String(bloomFilter));
         ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
      this.bloomFilter = (BloomFilter) objectInputStream.readObject();
    }
    catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException(e);
    }
    this.atomicUpdateGroupSize = atomicUpdateGroupSize;

    checkInput();
  }

  public BloomFilterNameNumberedOverwriteShardSpec(
      int partitionNum,
      int partitions,
      @Nullable String partitionName,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion,
      String dimension,
      List<Long> values,
      short atomicUpdateGroupSize
  )
  {
    super(partitionNum, partitions, partitionName);
    this.startRootPartitionId = (short) startRootPartitionId;
    this.endRootPartitionId = (short) endRootPartitionId;
    this.minorVersion = minorVersion;
    this.dimension = dimension;
    this.domainDimensions = ImmutableList.of(dimension);
    this.bloomFilter = BloomFilter.create(Funnels.longFunnel(), values.size());
    values.forEach(v -> this.bloomFilter.put(v));
    this.atomicUpdateGroupSize = atomicUpdateGroupSize;

    checkInput();
  }

  @JsonProperty("bloomFilter")
  public String getBloomFilter()
  {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(bloomFilter);
      return StringUtils.encodeBase64String(byteArrayOutputStream.toByteArray());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return this.dimension;
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return domainDimensions;
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    if (!domain.containsKey(dimension)) {
      return true;
    }
    // Only support the range set that contains just 1 long element
    Set<Range<String>> ranges = domain.get(dimension).asRanges();
    if (ranges.size() != 1) {
      return true;
    }
    Range<String> r = ranges.iterator().next();
    try {
      if (r.lowerEndpoint() != r.upperEndpoint()) {
        return true;
      }
      return bloomFilter.mightContain(Long.valueOf(r.lowerEndpoint()));
    }
    catch (Exception unused) {
      return true;
    }
  }

  private void checkInput()
  {
    Preconditions.checkArgument(
        this.getPartitionNum() >= PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
            && this.getPartitionNum() < PartitionIds.NON_ROOT_GEN_END_PARTITION_ID,
        "partitionNum[%s] >= %s && partitionNum[%s] < %s",
        this.getPartitionNum(),
        PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
        this.getPartitionNum(),
        PartitionIds.NON_ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(
        startRootPartitionId >= PartitionIds.ROOT_GEN_START_PARTITION_ID
            && startRootPartitionId < PartitionIds.ROOT_GEN_END_PARTITION_ID,
        "startRootPartitionId[%s] >= %s && startRootPartitionId[%s] < %s",
        startRootPartitionId,
        PartitionIds.ROOT_GEN_START_PARTITION_ID,
        startRootPartitionId,
        PartitionIds.ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(
        endRootPartitionId >= PartitionIds.ROOT_GEN_START_PARTITION_ID
            && endRootPartitionId < PartitionIds.ROOT_GEN_END_PARTITION_ID,
        "endRootPartitionId[%s] >= %s && endRootPartitionId[%s] < %s",
        endRootPartitionId,
        PartitionIds.ROOT_GEN_START_PARTITION_ID,
        endRootPartitionId,
        PartitionIds.ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(minorVersion > 0, "minorVersion[%s] > 0", minorVersion);
    Preconditions.checkArgument(
        atomicUpdateGroupSize > 0 || atomicUpdateGroupSize == PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE,
        "atomicUpdateGroupSize[%s] > 0 or == %s",
        atomicUpdateGroupSize,
        PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE
    );
  }

  @Override
  public OverwriteShardSpec withAtomicUpdateGroupSize(short atomicUpdateGroupSize)
  {
    return new BloomFilterNameNumberedOverwriteShardSpec(
        this.getPartitionNum(),
        this.getNumCorePartitions(),
        this.getPartitionName(),
        this.getStartRootPartitionId(),
        this.getEndRootPartitionId(),
        this.getMinorVersion(),
        this.getDimension(),
        this.getBloomFilter(),
        atomicUpdateGroupSize
    );
  }

}
