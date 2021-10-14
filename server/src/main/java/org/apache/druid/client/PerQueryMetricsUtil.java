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

package org.apache.druid.client;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.SegmentDescriptor;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The util class for calculating per-query metrics.
 */
public class PerQueryMetricsUtil
{
  /**
   * Gets a string to represent the segment count grouped by tiers.
   * Example format: tier1:10,tier2:20,tier3:30
   */
  public static String getSegmentCountByTiers(
      SortedMap<DruidServer, Pair<List<SegmentDescriptor>, SortedMap<DruidServer,
          List<SegmentDescriptor>>>> segmentsByServer)
  {
    SortedMap<String, Integer> segmentCountByTiers = new TreeMap<>(
        segmentsByServer.entrySet().stream().collect(
            Collectors.groupingBy(entry -> entry.getKey().getTier(),
                Collectors.summingInt(entry -> entry.getValue().lhs.size()))
        ));
    return segmentCountByTiers.entrySet().stream()
        .map(entry -> entry.getKey() + ":" + entry.getValue())
        .collect(Collectors.joining(","));
  }
}
