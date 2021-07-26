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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

public class BloomFilterNameNumberedOverwriteShardSpecFactory implements ShardSpecFactory
{

  private final int partitionNum;
  private final int partitions;
  private final String partitionName;
  private final int startRootPartitionId;
  private final int endRootPartitionId;
  private final short minorVersion;
  private final String dimension;
  private final String bloomFilter;

  public BloomFilterNameNumberedOverwriteShardSpecFactory(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionName") @Nullable String partitionName,
      @JsonProperty("startRootPartitionId") int startRootPartitionId,
      @JsonProperty("endRootPartitionId") int endRootPartitionId,
      @JsonProperty("minorVersion") short minorVersion,
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bloomFilter") String bloomFilter
  )
  {
    this.partitionNum = partitionNum;
    this.partitions = partitions;
    this.partitionName = partitionName;
    this.startRootPartitionId = startRootPartitionId;
    this.endRootPartitionId = endRootPartitionId;
    this.minorVersion = minorVersion;
    this.dimension = dimension;
    this.bloomFilter = bloomFilter;
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    return new BloomFilterNameNumberedOverwriteShardSpec(
        specOfPreviousMaxPartitionId == null
            ? PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
            : specOfPreviousMaxPartitionId.getPartitionNum() + 1,
        partitions,
        partitionName,
        startRootPartitionId,
        endRootPartitionId,
        minorVersion,
        dimension,
        bloomFilter,
        PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE
    );
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, int partitionId)
  {
    return new BloomFilterNameNumberedOverwriteShardSpec(
      partitionId,
      partitions,
      partitionName,
      startRootPartitionId,
      endRootPartitionId,
      minorVersion,
      dimension,
      bloomFilter,
      PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE
  );
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return BloomFilterNameNumberedOverwriteShardSpec.class;
  }
}
