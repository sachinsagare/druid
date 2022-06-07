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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class PerQueryMetricsUtilTest
{
  @Test
  public void testGetSegmentCountByTiersAndNamespaces()
  {
    SortedMap<DruidServer, Pair<List<SegmentDescriptor>, SortedMap<DruidServer,
        List<SegmentDescriptor>>>> segmentsByServer = new TreeMap<>();

    segmentsByServer.put(
        new DruidServer("server1", "host1:111", "host1:222", 10, ServerType.BRIDGE, "tier1", 0),
        new Pair<>(Arrays.asList(
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 1, "namespace1_1"),
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 2, "namespace1_2")), null));

    segmentsByServer.put(
        new DruidServer("server2", "host2:111", "host2:222", 10, ServerType.BRIDGE, "tier2", 0),
        new Pair<>(Arrays.asList(
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 3, "namespace1_3"),
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 4, "namespace2_4"),
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 5, "namespace2_5")), null));
    segmentsByServer.put(
        new DruidServer("server3", "host3:111", "host3:222", 10, ServerType.BRIDGE, "tier1", 0),
        new Pair<>(Arrays.asList(
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 6, "namespace1_6"),
            new SegmentDescriptor(Intervals.utc(1L, 2L), "v1", 7, "namespace2_7")), null));

    String segmentsCountByTiersAndNamespaces =
        PerQueryMetricsUtil.getSegmentCountStats(segmentsByServer);
    Assert.assertEquals("tier1:namespace1=3,namespace2=1;tier2:namespace1=1,namespace2=2",
        segmentsCountByTiersAndNamespaces);
  }
}
