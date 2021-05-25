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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public enum NilColumnValueSelector implements ColumnValueSelector
{

  INSTANCE;

  @Override
  public double getDouble()
  {
    return 0.0d;
  }

  @Override
  public float getFloat()
  {
    return 0.0f;
  }

  @Override
  public long getLong()
  {
    return 0L;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // do nothing
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
    return Object.class;
  }

  @Override
  public boolean isNull()
  {
    return true;
  }

}