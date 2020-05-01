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

package org.apache.druid.query.aggregation.datasketches.frequencies.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.frequencies.LongsSketchModule;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LongsSketchSqlAggregatorTest extends CalciteTestBase
{
  private static final String DATA_SOURCE = "longs_sketch_test_data_source";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, "dummy"
  );

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setUp() throws Exception
  {
    LongsSketchModule.registerSerde();
    for (Module mod : new LongsSketchModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
    }

    // Create input rows
    File inputFile = new File(this.getClass().getClassLoader().getResource("frequencies/frequencies_longs_raw.tsv").getFile());
    InputStream inputStream = new FileInputStream(inputFile);
    LineIterator iter = IOUtils.lineIterator(inputStream, "UTF-8");
    String parserJson = String.join("\n",
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
        "}");
    StringInputRowParser parser = TestHelper.makeJsonMapper().readValue(parserJson, StringInputRowParser.class);

    List<InputRow> rows = new ArrayList<>();
    while (iter.hasNext()) {
      String row = iter.next();
      rows.add(parser.parse(row));
    }

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new LongsSketchBuildAggregatorFactory(
                                                             "dim1_longs_sketch",
                                                             "dim1",
                                                             8,
                                                             null,
                                                             null
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(rows)
                                             .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new LongsSketchSqlAggregator()),
        ImmutableSet.of()
    );

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            druidSchema,
            systemSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper()
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testApproxFrequencyLongsSketch() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_FREQUENCY_LONGS(dim1, 8, 'NO_FALSE_NEGATIVES', 1),\n"
        + "  APPROX_FREQUENCY_LONGS(dim1, 8, 'NO_FALSE_NEGATIVES'),\n"
        + "  APPROX_FREQUENCY_LONGS(dim1, 8),\n"
        + "  APPROX_FREQUENCY_LONGS(dim1),\n"
        + "  APPROX_FREQUENCY_LONGS(dim1_longs_sketch, 8, 'NO_FALSE_NEGATIVES')\n" // on native longs sketch column
        + "FROM druid." + DATA_SOURCE;

    // Verify results
    final List<Object[]> actualResults = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, authenticationResult).toList();
    Assert.assertTrue(!actualResults.isEmpty());

    String expectedApproxFrequency = "[{\"Est\":4551,\"UB\":4551,\"LB\":57,\"Item\":97},{\"Est\":4549,\"UB\":4549,\"LB\":55,\"Item\":99},{\"Est\":4531,\"UB\":4531,\"LB\":37,\"Item\":98},{\"Est\":4527,\"UB\":4527,\"LB\":33,\"Item\":95},{\"Est\":4526,\"UB\":4526,\"LB\":32,\"Item\":96}]";
    final List<Object[]> expectedResults = ImmutableList.of(
          new Object[]{
              20000L,
              expectedApproxFrequency,
              expectedApproxFrequency,
              expectedApproxFrequency,
              expectedApproxFrequency,
              expectedApproxFrequency
          }
      );

    Assert.assertEquals(expectedResults.size(), actualResults.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), actualResults.get(i));
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(DATA_SOURCE)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new LongSumAggregatorFactory("a0", "cnt"),
                      new LongsSketchBuildAggregatorFactory(
                          "a1",
                          "dim1",
                          8,
                          "NO_FALSE_NEGATIVES",
                          1L
                      ),
                      new LongsSketchBuildAggregatorFactory(
                          "a2",
                          "dim1",
                          8,
                          "NO_FALSE_NEGATIVES",
                          null
                      ),
                      new LongsSketchBuildAggregatorFactory(
                          "a3",
                          "dim1",
                          8,
                          null,
                          null
                      ),
                      new LongsSketchBuildAggregatorFactory(
                          "a4",
                          "dim1",
                          null,
                          null,
                          null
                      ),
                      new LongsSketchMergeAggregatorFactory(
                          "a5",
                          "dim1_longs_sketch",
                          8,
                          "NO_FALSE_NEGATIVES",
                          null
                      )
                  )
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }
}
