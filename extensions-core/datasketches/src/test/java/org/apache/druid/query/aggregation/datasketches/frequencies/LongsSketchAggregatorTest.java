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

package org.apache.druid.query.aggregation.datasketches.frequencies;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class LongsSketchAggregatorTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public LongsSketchAggregatorTest(GroupByQueryConfig config)
  {
    LongsSketchModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new LongsSketchModule().getJacksonModules(), config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[] {config});
    }
    return constructors;
  }

  @Test
  public void ingestSketches() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("frequencies/frequencies_longs_sketches.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"dim1\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim1\", \"sketch\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"LongsSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"LongsSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("[{\"Est\":4110,\"UB\":4110,\"LB\":67,\"Item\":97},{\"Est\":4098,\"UB\":4098,\"LB\":55,\"Item\":99},{\"Est\":4081,\"UB\":4081,\"LB\":38,\"Item\":98},{\"Est\":4059,\"UB\":4059,\"LB\":16,\"Item\":94},{\"Est\":4052,\"UB\":4052,\"LB\":9,\"Item\":93},{\"Est\":4049,\"UB\":4049,\"LB\":6,\"Item\":92}]", row.get(0));
  }

  @Test
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("frequencies/frequencies_longs_raw.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim1\", \"dim2\"]",
            "  }",
            "}"),
        String.join("\n",
            "[",
            "  {\"type\": \"LongsSketchBuild\", \"name\": \"sketch\", \"fieldName\": \"dim1\", \"maxMapSize\": 8},",
            "    {\"type\": \"longSum\", \"name\": \"dim2_sum\", \"fieldName\": \"dim2\"}",
            "]"),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"LongsSketchMerge\", \"name\": \"sketch\", \"fieldName\": \"sketch\", \"maxMapSize\": 8, \"errorType\": \"NO_FALSE_NEGATIVES\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("[{\"Est\":4551,\"UB\":4551,\"LB\":57,\"Item\":97},{\"Est\":4549,\"UB\":4549,\"LB\":55,\"Item\":99},{\"Est\":4531,\"UB\":4531,\"LB\":37,\"Item\":98},{\"Est\":4527,\"UB\":4527,\"LB\":33,\"Item\":95},{\"Est\":4526,\"UB\":4526,\"LB\":32,\"Item\":96}]", row.get(0));
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("frequencies/frequencies_longs_raw.tsv").getFile()),
        String.join("\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMdd\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"dim1\", \"dim2\"]",
            "  }",
            "}"),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        String.join("\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "    {\"type\": \"LongsSketchBuild\", \"name\": \"sketch\", \"fieldName\": \"dim1\", \"maxMapSize\": 8, \"errorType\": \"NO_FALSE_NEGATIVES\"}",
            "  ],",
            "  \"intervals\": [\"2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z\"]",
            "}"));
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("[{\"Est\":4551,\"UB\":4551,\"LB\":57,\"Item\":97},{\"Est\":4549,\"UB\":4549,\"LB\":55,\"Item\":99},{\"Est\":4531,\"UB\":4531,\"LB\":37,\"Item\":98},{\"Est\":4527,\"UB\":4527,\"LB\":33,\"Item\":95},{\"Est\":4526,\"UB\":4526,\"LB\":32,\"Item\":96}]", row.get(0));
  }
}
