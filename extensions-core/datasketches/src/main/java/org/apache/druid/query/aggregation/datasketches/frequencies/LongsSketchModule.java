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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.yahoo.sketches.frequencies.LongsSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.frequencies.longssketch.LongsSketchWrap;
import org.apache.druid.query.aggregation.datasketches.frequencies.sql.LongsSketchSqlAggregator;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

/**
 * This module is to support frequency count operations for longs using {@link LongsSketch}.
 */
public class LongsSketchModule implements DruidModule
{
  public static final String TYPE_NAME = "LongsSketch"; // common type name to be associated with segment data
  public static final String BUILD_TYPE_NAME = "LongsSketchBuild";
  public static final String MERGE_TYPE_NAME = "LongsSketchMerge";

  @Override
  public void configure(final Binder binder)
  {
    registerSerde();
    SqlBindings.addAggregator(binder, LongsSketchSqlAggregator.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("LongsSketchModule").registerSubtypes(
            new NamedType(LongsSketchMergeAggregatorFactory.class, MERGE_TYPE_NAME),
            new NamedType(LongsSketchBuildAggregatorFactory.class, BUILD_TYPE_NAME),
            new NamedType(LongsSketchMergeAggregatorFactory.class, TYPE_NAME)
        ).addSerializer(LongsSketchWrap.class, new LongsSketchJsonSerializer())
    );
  }

  @VisibleForTesting
  public static void registerSerde()
  {
    if (ComplexMetrics.getSerdeForType(TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(TYPE_NAME, new LongsSketchMergeComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(BUILD_TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(BUILD_TYPE_NAME, new LongsSketchBuildComplexMetricSerde());
    }
    if (ComplexMetrics.getSerdeForType(MERGE_TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(MERGE_TYPE_NAME, new LongsSketchMergeComplexMetricSerde());
    }
  }
}
