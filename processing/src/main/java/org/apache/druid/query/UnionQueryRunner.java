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

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;

import java.util.List;
import java.util.Optional;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof UnionDataSource) {

      return new MergeSequence<>(
          query.getResultOrdering(),
          Sequences.simple(
              Lists.transform(
                  ((UnionDataSource) dataSource).getDataSources(),
                  new Function<TableDataSource, Sequence<T>>()
                  {
                    @Override
                    public Sequence<T> apply(TableDataSource singleSource)
                    {
                      Optional<List<AggregatorFactory>> aggs = ((UnionDataSource) dataSource).getOverrideAggregators(
                          singleSource.getName());
                      // Since we are overriding the aggregator, we could run into a situation that a post aggregator
                      // would be missing dependency. The check for dependency should have happened at the base query
                      Query<T> newQuery =
                          aggs.isPresent()
                          ? QueryContexts.withIgnoreMissingDepPostAgg(query).withAggregatorSpecs(aggs.get())
                          : query;
                      return baseRunner.run(
                          queryPlus.withQuery(newQuery.withDataSource(singleSource)),
                          responseContext
                      );
                    }
                  }
              )
          )
      );
    } else {
      return baseRunner.run(queryPlus, responseContext);
    }
  }

}
