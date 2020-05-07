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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class NoopAggregatorFactory extends AggregatorFactory
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return 0;
    }
  };
  static final AggregateCombiner COMBINER = new AggregateCombiner()
  {
    @Override
    public void reset(ColumnValueSelector selector)
    {
    }

    @Override
    public void fold(ColumnValueSelector selector)
    {
    }

    @Override
    public double getDouble()
    {
      return 0;
    }

    @Override
    public float getFloat()
    {
      return 0;
    }

    @Override
    public long getLong()
    {
      return 0;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return null;
    }

    @Override
    public Class classOfObject()
    {
      return null;
    }
  };

  private final String name;
  private final String typeName;
  private final boolean isNumber;

  public NoopAggregatorFactory(
      String name,
      String typeName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.name = name;
    this.typeName = typeName;
    // This needs to change when we have support column types/aggregator types better
    this.isNumber = "float".equals(typeName) || "long".equals(typeName) || "double".equals(typeName);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (isNumber) {
      return NoopNumberAggregator.instance();
    }
    return NoopAggregator.instance();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (isNumber) {
      return NoopNumberBufferAggregator.instance();
    }
    return NoopBufferAggregator.instance();
  }

  @Override
  public VectorAggregator factorizeVector(final VectorColumnSelectorFactory selectorFactory)
  {
    if (isNumber) {
      return NoopNumberVectorAggregator.instance();
    }
    return NoopVectorAggregator.instance();
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  /*@Override*/
  public boolean canVectorize()
  {
    return true;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return lhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return COMBINER;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new NoopAggregatorFactory(name, typeName);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new NoopAggregatorFactory(name, typeName));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of();
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  /*@Override*/
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "NoopAggregatorFactory{" +
           "name='" + name + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NoopAggregatorFactory that = (NoopAggregatorFactory) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return name != null ? name.hashCode() : 0;
  }
}
