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

package org.apache.druid.query.aggregation.collectset;

import java.util.Collection;
import java.util.Set;

public class CollectSetUtil
{
  private CollectSetUtil()
  {
  }

  // add value into set and make sure set size is within the limit.
  public static void addWithLimit(Set<Object> set, Object value, int limit)
  {
    // negative limit means unlimited.
    if (limit >= 0 && set.size() >= limit) {
      return;
    }

    int valSize = ((Collection) value).size();
    if (limit >= 0 && valSize > limit - set.size()) {
      // make sure not to exceed the limit.
      for (Object v : (Collection) value) {
        set.add(v);
        if (set.size() >= limit) {
          return;
        }
      }
    } else {
      set.addAll((Collection) value);
    }
  }
}
