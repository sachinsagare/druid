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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class StreamHashBasedNumberedShardSpec extends HashBasedNumberedShardSpec
{
  private final ObjectMapper jsonMapper;

  @JsonIgnore
  private final Integer streamPartitions;

  @JsonIgnore
  private final Set<Integer> streamPartitionIds;

  @JsonCreator
  public StreamHashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JsonProperty("streamPartitionIds") @Nullable Set<Integer> streamPartitionIds,
      @JsonProperty("streamPartitions") @Nullable Integer streamPartitions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions, partitionDimensions, jsonMapper);
    this.streamPartitionIds = streamPartitionIds == null ? ImmutableSet.of() : streamPartitionIds;
    this.streamPartitions = streamPartitions;
    this.jsonMapper = jsonMapper;
  }

  @JsonProperty("streamPartitionIds")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Set<Integer> getStreamPartitionIds()
  {
    return streamPartitionIds;
  }

  @JsonProperty("streamPartitions")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getStreamPartitions()
  {
    return streamPartitions;
  }

  @Override
  public boolean isCompatible(Class<? extends ShardSpec> other)
  {
    return other == NumberedShardSpec.class ||
           other == NumberedOverwriteShardSpec.class ||
           other == StreamHashBasedNumberedShardSpec.class ||
           other == StreamFanOutHashBasedNumberedShardSpec.class ||
           other == BloomFilterStreamFanOutHashBasedNumberedShardSpec.class;
  }

  @Override
  public String toString()
  {
    return "StreamHashBasedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getPartitions() +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (!(o instanceof StreamHashBasedNumberedShardSpec)) {
      return false;
    }

    final StreamHashBasedNumberedShardSpec that = (StreamHashBasedNumberedShardSpec) o;
    if (getPartitionNum() != that.getPartitionNum()) {
      return false;
    }
    return getPartitions() == that.getPartitions();
  }

  @Override
  public int hashCode()
  {
    // partitionDimensions, streamPartitions and streamPartitionIds are intentionally left unincluded in equals,
    // hashCode and toString.
    return Objects.hash(getPartitionNum(), getPartitions());
  }

  @Override
  protected boolean groupKeyIsInChunk(List<Object> groupKey)
  {
    try {
      return this.streamPartitionIds.contains(Math.abs(hash(jsonMapper, groupKey) % getStreamPartitions()));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
