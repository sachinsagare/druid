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

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.partition.OvershadowableInteger;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class NamespacedVersionedIntervalTimelineTest
{
  private static final String NAMESPACE1 = "ns1_2020-01-01";
  private static final String PARENT_NAMESPACE1 = "ns1";
  private static final String NAMESPACE2 = "ns2";

  private NamespacedVersionedIntervalTimeline<String, OvershadowableInteger> timeline;

  @Before
  public void setUp()
  {
    timeline = new NamespacedVersionedIntervalTimeline<>();
    OverShadowConfigProvider.getInstance().inject(new OverShadowConfig()
    {
      @Override
      public Map<String, String> getNamespaceChildParentMap()
      {
        return ImmutableMap.of("ns1.*", PARENT_NAMESPACE1);
      }
    });
    add(timeline, NAMESPACE1, "2019-09-01/2019-09-03", new OvershadowableInteger("0", 0, 1));
    add(timeline, PARENT_NAMESPACE1, "2019-09-06/2019-09-09", new OvershadowableInteger("0", 0, 1));
  }

  @Test
  public void testOvershadow()
  {
    Assert.assertFalse(timeline.isOvershadowed(
        NAMESPACE1,
        Intervals.of("2019-01-01/2019-01-02"),
        "0",
        new OvershadowableInteger("0", 0, 1)
    ));
    Assert.assertTrue(timeline.isOvershadowed(
        NAMESPACE1,
        Intervals.of("2019-09-02/2019-09-03"),
        "0",
        new OvershadowableInteger("0", 0, 1)
    ));
  }

  @Test
  public void testNotOvershadowedByParentNamespace()
  {
    Assert.assertFalse(timeline.isOvershadowed(
        NAMESPACE1,
        Intervals.of("2019-01-01/2019-01-02"),
        "0",
        new OvershadowableInteger("0", 0, 1)
    ));
    Assert.assertFalse(timeline.isOvershadowed(
        NAMESPACE2,
        Intervals.of("2019-09-02/2019-09-03"),
        "0",
        new OvershadowableInteger("0", 0, 1)
    ));
  }

  @Test
  public void testOvershadowedByParentNamespace()
  {
    Assert.assertTrue(timeline.isOvershadowed(
        NAMESPACE1,
        Intervals.of("2019-09-06/2019-09-07"),
        "0",
        new OvershadowableInteger("0", 0, 1)
    ));
  }

  private void add(
      NamespacedVersionedIntervalTimeline timelineToAdd,
      String namespace,
      String intervalString,
      OvershadowableInteger value
  )
  {
    Interval interval = Intervals.of(intervalString);
    timelineToAdd.add(namespace, interval, "1.0", new SingleElementPartitionChunk<>(value));
  }

}
