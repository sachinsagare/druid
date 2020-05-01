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

package org.apache.druid.query.aggregation.datasketches.frequencies.longssketch;


/**
 *  Copied unmodified (except styles to pass lint) from sketches-core-0.13.4
 *  The reason we are not able to take external jar as dependency is that this is a package level
 *  private class but it is needed for implementing LongsSketchWrap that has direct memory interface
 *  support to fit into druid's aggregator interface. The long term solution is to remove this class
 *  after the upstream adds the direct memory interface support.
 *  */
final class Util
{
  private Util()
  {
  }

  /**
   * The following constant controls the size of the initial data structure for the
   * frequencies sketches and its value is somewhat arbitrary.
   */
  static final int LG_MIN_MAP_SIZE = 3;

  /**
   * This constant is large enough so that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the
   * true median with high probability.
   */
  static final int SAMPLE_SIZE = 1024;

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of
   * Austin Appleby's MurmurHash3 algorithm. It is also used by the Trove for Java libraries.
   */
  static long hash(long key)
  {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }
}
