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

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.QuickSelect;
import com.yahoo.sketches.SketchesArgumentException;
import org.apache.druid.java.util.common.StringUtils;


/**
 * Adapted from ReversePurgeLongHashMap.java in sketches-core-0.13.4
 * The reason we are not able to take external jar as dependency is that the ReversePurgeLongHashMap
 * in upstream does not have a direct memory interface support to fit into druid's aggregator
 * interface so here we implement our own first. The long term solution is to remove this class
 * after the upstream adds the direct memory interface support.
 *
 * ReversePurgeLongHashMap is the main data structure for longs sketch.
 */
public class DirectReversePurgeLongHashMap
{
  private static final double LOAD_FACTOR = 0.75;
  private static final int DRIFT_LIMIT = 1024; //used only in stress testing
  public static final int KEY_SIZE = Long.BYTES;
  public static final int VALUE_SIZE = Long.BYTES;
  private static final int STATE_SIZE = Short.BYTES;

  private int lgLength;
  private int loadThreshold;
  private int numActive = 0;
  private int mapSize;
  private WritableMemory keys;
  private WritableMemory values;
  private WritableMemory states;
  // Read-only
  private Memory rKeys;
  private Memory rValues;

  /**
   * Constructor will create hash map of size maxMapSize, which must be a power of two. This
   * restriction was made to ensure fast hashing.
   * The protected variable this.loadThreshold is then set to the largest value that
   * will not overload the hash table.
   *
   * Note that no resize function is provided for the current implementation.
   *
   * @param mapSize The max number of cells in the arrays underlying the
   * HashMap implementation and must be a power of 2.
   * @param wmem The writable memory backing the arrays underlying the HashMap implementation
   *
   * The hash table will be expected to store LOAD_FACTOR * mapMaxSize (key, value) pairs
   */
  public DirectReversePurgeLongHashMap(final int mapSize, WritableMemory wmem)
  {
    // Memory layout:
    // | values | keys | states |
    // Verify that the given writable memory has sufficient space
    long maxBytesNeeded = (VALUE_SIZE + KEY_SIZE + STATE_SIZE) * mapSize;

    if (maxBytesNeeded > wmem.getCapacity()) {
      throw new SketchesArgumentException("Insufficient space, bytes needed: " + maxBytesNeeded + ", bytes available: " + wmem.getCapacity());
    }

    this.lgLength = com.yahoo.sketches.Util.toLog2(mapSize, "mapSize");
    this.loadThreshold = (int) (mapSize * LOAD_FACTOR);
    this.mapSize = mapSize;
    this.numActive = 0;
    this.values = wmem.writableRegion(0, VALUE_SIZE * mapSize);
    this.keys = wmem.writableRegion(VALUE_SIZE * mapSize, KEY_SIZE * mapSize);
    this.states = wmem.writableRegion(VALUE_SIZE * mapSize + KEY_SIZE * mapSize, STATE_SIZE * mapSize);
  }

  public DirectReversePurgeLongHashMap(final int mapSize, final int numActive, Memory mem)
  {
    this.lgLength = com.yahoo.sketches.Util.toLog2(mapSize, "mapSize");
    this.loadThreshold = (int) (mapSize * LOAD_FACTOR);
    this.mapSize = mapSize;
    this.numActive = numActive;
    // For read-only mode, the serialized bytes are in compact mode so only numActive items are
    // serialized
    if (mem != null) {
      this.rValues = mem.region(0, VALUE_SIZE * numActive);
      this.rKeys = mem.region(VALUE_SIZE * numActive, KEY_SIZE * numActive);
    }
  }

  /**
   * @return Size in bytes for the backing array of the hashmap as a function of mapSize
   */
  static int getMaxStorageBytes(int maxMapSize)
  {
    return (KEY_SIZE + VALUE_SIZE + STATE_SIZE) * maxMapSize;
  }

  /**
   * @param probe location in the hash table array
   * @return true if the cell in the array contains an active key
   */
  private boolean isActive(final int probe)
  {
    return (states.getShort(probe * STATE_SIZE) > 0);
  }

  /**
   * Gets the current value with the given key
   * @param key the given key
   * @return the positive value the key corresponds to or zero if the key is not found in the
   * hash map.
   */
  long get(final long key)
  {
    final int probe = hashProbe(key);
    if (rKeys != null || states.getShort(probe * STATE_SIZE) > 0) {
      assert (keys.getLong(probe * KEY_SIZE) == key);
      return values.getLong(probe * VALUE_SIZE);
    }
    return 0;
  }

  /**
   * Increments the value mapped to the key if the key is present in the map. Otherwise,
   * the key is inserted with the putAmount.
   *
   * @param key the key of the value to increment
   * @param adjustAmount the amount by which to increment the value
   */
  void adjustOrPutValue(final long key, final long adjustAmount)
  {
    final int arrayMask = mapSize - 1;
    int probe = (int) Util.hash(key) & arrayMask;
    int drift = 1;

    while ((states.getShort(probe * STATE_SIZE) != 0) && (keys.getLong(probe * KEY_SIZE) != key)) {
      probe = (probe + 1) & arrayMask;
      drift++;
      //only used for theoretical analysis
      assert (drift < DRIFT_LIMIT) : "drift: " + drift + " >= DRIFT_LIMIT";
    }
    //found either an empty slot or the key
    if (states.getShort(probe * STATE_SIZE) == 0) { //found empty slot
      // adding the key and value to the table
      assert (numActive <= loadThreshold)
          : "numActive: " + numActive + " > loadThreshold : " + loadThreshold;
      keys.putLong(probe * KEY_SIZE, key);
      values.putLong(probe * VALUE_SIZE, adjustAmount);
      states.putShort(probe * STATE_SIZE, (short) drift); //how far off we are
      numActive++;
    } else { //found the key, adjust the value
      assert (keys.getLong(probe * KEY_SIZE) == key);
      values.getAndAddLong(probe * VALUE_SIZE, adjustAmount);
    }
  }

  /**
   * Processes the map arrays and retains only keys with positive counts.
   */
  private void keepOnlyPositiveCounts()
  {
    // Starting from the back, find the first empty cell, which marks a boundary between clusters.
    int firstProbe = mapSize - 1;
    while (states.getShort(firstProbe * STATE_SIZE) > 0) {
      firstProbe--;
    }

    //Work towards the front; delete any non-positive entries.
    for (int probe = firstProbe; probe-- > 0; ) {
      // When we find the next non-empty cell, we know we are at the high end of a cluster,
      //  which is tracked by firstProbe.
      if ((states.getShort(probe * STATE_SIZE) > 0) && (values.getLong(probe * VALUE_SIZE) <= 0)) {
        hashDelete(probe); //does the work of deletion and moving higher items towards the front.
        numActive--;
      }
    }
    //now work on the first cluster that was skipped.
    for (int probe = mapSize; probe-- > firstProbe;) {
      if ((states.getShort(probe * STATE_SIZE) > 0) && (values.getLong(probe * VALUE_SIZE) <= 0)) {
        hashDelete(probe);
        numActive--;
      }
    }
  }

  /**
   * @param adjustAmount value by which to shift all values. Only keys corresponding to positive
   * values are retained.
   */
  void adjustAllValuesBy(final long adjustAmount)
  {
    for (int i = mapSize; i-- > 0; ) {
      values.getAndAddLong(i * VALUE_SIZE, adjustAmount);
    }
  }

  /**
   * @return an array containing the active keys in the hash map.
   */
  long[] getActiveKeys()
  {
    if (numActive == 0) {
      return null;
    }
    final long[] returnedKeys = new long[numActive];
    int j = 0;
    for (int i = 0; i < mapSize; i++) {
      if (isActive(i)) {
        returnedKeys[j] = keys.getLong(i * KEY_SIZE);
        j++;
      }
    }
    assert (j == numActive) : "j: " + j + " != numActive: " + numActive;
    return returnedKeys;
  }

  /**
   * @return an array containing the values corresponding. to the active keys in the hash
   */
  long[] getActiveValues()
  {
    if (numActive == 0) {
      return null;
    }
    final long[] returnedValues = new long[numActive];
    int j = 0;
    for (int i = 0; i < mapSize; i++) {
      if (isActive(i)) {
        returnedValues[j] = values.getLong(i * VALUE_SIZE);
        j++;
      }
    }
    assert (j == numActive);
    return returnedValues;
  }

  /**
   * @return length of hash table internal arrays
   */
  int getLength()
  {
    return mapSize;
  }

  int getLgLength()
  {
    return lgLength;
  }

  /**
   * @return capacity of hash table internal arrays (i.e., max number of keys that can be stored)
   */
  public int getCapacity()
  {
    return loadThreshold;
  }

  /**
   * @return number of populated keys
   */
  int getNumActive()
  {
    return numActive;
  }

  /**
   * Returns the hash table as a human readable string.
   */
  @Override
  public String toString()
  {
    final String fmt = " %12d:%11d%20d %d";
    final String hfmt = " %12s:%11s%20s %s";
    final StringBuilder sb = new StringBuilder();
    sb.append("DirectReversePurgeLongHashMap:").append(com.yahoo.sketches.Util.LS);
    sb.append(StringUtils.format(hfmt, "Index", "States", "Values", "Keys")).append(com.yahoo.sketches.Util.LS);

    for (int i = 0; i < mapSize; i++) {
      if (states.getShort(i * STATE_SIZE) <= 0) {
        continue;
      }
      sb.append(StringUtils.format(fmt, i, states.getShort(i * STATE_SIZE), values.getLong(i * VALUE_SIZE), keys.getLong(i * KEY_SIZE))).append(com.yahoo.sketches.Util.LS);
    }
    return sb.toString();
  }

  /**
   * @return the load factor of the hash table, i.e, the ratio between the capacity and the array
   * length
   */
  static double getLoadFactor()
  {
    return LOAD_FACTOR;
  }

  /**
   * This function is called when a key is processed that is not currently assigned a counter, and
   * all the counters are in use. This function estimates the median of the counters in the sketch
   * via sampling, decrements all counts by this estimate, throws out all counters that are no
   * longer positive, and increments offset accordingly.
   * @param sampleSize number of samples
   * @return the median value
   */
  long purge(final int sampleSize)
  {
    final int limit = Math.min(sampleSize, getNumActive());

    int numSamples = 0;
    int i = 0;
    final long[] samples = new long[limit];

    while (numSamples < limit) {
      if (isActive(i)) {
        samples[numSamples] = values.getLong(i * VALUE_SIZE);
        numSamples++;
      }
      i++;
    }

    final long val = QuickSelect.select(samples, 0, numSamples - 1, limit / 2);
    adjustAllValuesBy(-1 * val);
    keepOnlyPositiveCounts();
    return val;
  }

  private void hashDelete(int deleteProbe)
  {
     // Looks ahead in the table to search for another item to move to this location.
    // If none are found, the status is changed
    states.putShort(deleteProbe * STATE_SIZE, (short) 0); //mark as empty
    int drift = 1;
    final int arrayMask = mapSize - 1;
    int probe = (deleteProbe + drift) & arrayMask; //map length must be a power of 2
    // advance until you find a free location replacing locations as needed
    while (states.getShort(probe * STATE_SIZE) != 0) {
      if (states.getShort(probe * STATE_SIZE) > drift) {
        // move current element
        keys.putLong(deleteProbe * KEY_SIZE, keys.getLong(probe * KEY_SIZE));
        values.putLong(deleteProbe * VALUE_SIZE, values.getLong(probe * VALUE_SIZE));
        states.putShort(deleteProbe * STATE_SIZE, (short) (states.getShort(probe * STATE_SIZE) - drift));
        // marking the current probe location as deleted
        states.putShort(probe * STATE_SIZE, (short) 0);
        drift = 0;
        deleteProbe = probe;
      }
      probe = (probe + 1) & arrayMask;
      drift++;
      //only used for theoretical analysis
      assert (drift < DRIFT_LIMIT) : "drift: " + drift + " >= DRIFT_LIMIT";
    }
  }

  private int hashProbe(final long key)
  {
    final int arrayMask = mapSize - 1;
    int probe = (int) Util.hash(key) & arrayMask;
    while ((states.getShort(probe * STATE_SIZE) > 0) && (keys.getLong(probe * KEY_SIZE) != key)) {
      probe = (probe + 1) & arrayMask;
    }
    return probe;
  }

  Iterator iterator()
  {
    return new Iterator(keys, values, states, rKeys, rValues, mapSize, numActive);
  }

  // This iterator uses strides based on golden ratio to avoid clustering during merge
  static class Iterator
  {
    private static final double GOLDEN_RATIO_RECIPROCAL = (Math.sqrt(5) - 1) / 2; //.618...

    private final WritableMemory keys_;
    private final WritableMemory values_;
    private final WritableMemory states_;
    // Read-only
    private final Memory rKeys_;
    private final Memory rValues_;
    private final int numActive_;
    private final int stride_;
    private final int mask_;
    private int i_;
    private int count_;

    Iterator(final WritableMemory keys, final WritableMemory values, final WritableMemory states, final Memory rKeys, final Memory rValues, final int mapSize, final int numActive)
    {
      keys_ = keys;
      values_ = values;
      states_ = states;
      numActive_ = numActive;
      rKeys_ = rKeys;
      rValues_ = rValues;
      count_ = 0;
      stride_ = (int) (mapSize * GOLDEN_RATIO_RECIPROCAL) | 1;
      mask_ = mapSize - 1;
      i_ = rKeys != null ? -1 : -stride_;
    }

    boolean next()
    {
      if (rKeys_ != null) {
        // Skip stride for read-only mode
        i_++;
        if (count_ < numActive_) {
          count_++;
          return true;
        } else {
          return false;
        }
      } else {
        i_ = (i_ + stride_) & mask_;
        while (count_ < numActive_) {
          if (states_.getShort(i_ * STATE_SIZE) > 0) {
            count_++;
            return true;
          }
          i_ = (i_ + stride_) & mask_;
        }
        return false;
      }
    }

    long getKey()
    {
      return rKeys_ != null ? rKeys_.getLong(i_ * KEY_SIZE) : keys_.getLong(i_ * KEY_SIZE);
    }

    long getValue()
    {
      return rKeys_ != null ? rValues_.getLong(i_ * VALUE_SIZE) : values_.getLong(i_ * VALUE_SIZE);
    }
  }

}
