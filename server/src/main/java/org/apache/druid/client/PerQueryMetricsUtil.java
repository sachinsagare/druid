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
import org.apache.druid.timeline.ComplementaryNamespacedVersionedIntervalTimeline;

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
   * Gets a string to represent the segment count stats (grouped by namespaces and tiers).
   * Example format: tier1:namespace1=10,namespace2=20;tier2:namespace1=30
   */
  public static String getSegmentCountStats(
      SortedMap<DruidServer, Pair<List<SegmentDescriptor>, SortedMap<DruidServer,
          List<SegmentDescriptor>>>> segmentsByServer)
  {

    SortedMap<String, SortedMap<String, Integer>> segmentCountByTiersAndNamespaces =
        new TreeMap<>();

    segmentsByServer.forEach((server, segments) -> {
      String tier = server.getTier();
      segmentCountByTiersAndNamespaces.putIfAbsent(tier, new TreeMap<>());
      SortedMap<String, Integer> segmentCountByNamespaces =
          segmentCountByTiersAndNamespaces.get(tier);
      segments.lhs.forEach(segment -> {
        String namespace =
            ComplementaryNamespacedVersionedIntervalTimeline.getRootNamespace(
                segment.getPartitionIdentifier().toString());
        segmentCountByNamespaces.merge(namespace, 1, Integer::sum);
      });
    });

    return segmentCountByTiersAndNamespaces.entrySet().stream()
        .map(entry -> {
          String tier = entry.getKey();
          SortedMap<String, Integer> segmentCountByNamespaces = entry.getValue();

          String countByNamespacesStr = segmentCountByNamespaces.entrySet().stream()
              .map(countEntry -> {
                String namespace = countEntry.getKey();
                Integer count = countEntry.getValue();
                return namespace + "=" + count;
              })
              .collect(Collectors.joining(","));

          return tier + ":" + countByNamespacesStr;
        })
        .collect(Collectors.joining(";"));
  }
}
