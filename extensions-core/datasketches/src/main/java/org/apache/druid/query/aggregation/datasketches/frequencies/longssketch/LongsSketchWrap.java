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
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;
import com.yahoo.sketches.frequencies.ErrorType;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Adapted from LongsSketch.java in sketches-core-0.13.4
 * The reason we are not able to take external jar as dependency is that the LongsSketch in upstream
 * does not have a direct memory interface support to fit into druid's aggregator interface so here
 * we implement our own first. The long term solution is to remove this class after the upstream
 * adds the direct memory interface support.
 */
public class LongsSketchWrap
{
  public static final long DEFAULT_THRESHOLD = Long.MAX_VALUE;

  /**
   * Log2 Maximum length of the arrays internal to the hash map supported by the data
   * structure.
   */
  private int lgMaxMapSize;

  /**
   * The current number of counters supported by the hash map.
   */
  private int curMapCap; //the threshold to purge

  /**
   * Tracks the total of decremented counts.
   */
  private long offset = 0;

  /**
   * The sum of all frequencies of the stream so far.
   */
  private long streamLength = 0;

  /**
   * The maximum number of samples used to compute approximate median of counters when doing
   * decrement
   */
  private int sampleSize;

  private DirectReversePurgeLongHashMap directHashMap;

  private ReversePurgeLongHashMap hashMap;

  private boolean isDirectHashMap = false;

  private Memory mem;

  /**
   * @return A longs sketch that operates on heap
   */
  public LongsSketchWrap(final int maxMapSize)
  {
    int lgMaxMapSize = com.yahoo.sketches.Util.toLog2(maxMapSize, "maxMapSize");
    //set initial size of hash map
    this.lgMaxMapSize = Math.max(lgMaxMapSize, Util.LG_MIN_MAP_SIZE);
    this.hashMap = new ReversePurgeLongHashMap(1 << Util.LG_MIN_MAP_SIZE);
    this.curMapCap = hashMap.getCapacity();
    final int maxMapCap =
        (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
    offset = 0;
    sampleSize = Math.min(Util.SAMPLE_SIZE, maxMapCap);
  }

  /**
   * @return A longs sketch that wraps around read-only mem as the backing storage, mem must be of
   * valid data serialized form
   */
  public LongsSketchWrap(final Memory mem)
  {
    this.isDirectHashMap = true;
    this.mem = mem;
    // Check meta data to ensure it's a valid sketch
    // Even if the sketch doesn't have actual data, the meta data part should still not be empty
    final long pre0 = PreambleUtil.checkPreambleSize(mem); //make sure preamble will fit
    final int maxPreLongs = Family.FREQUENCY.getMaxPreLongs();
    final int preLongs = PreambleUtil.extractPreLongs(pre0);         //Byte 0
    final int serVer = PreambleUtil.extractSerVer(pre0);             //Byte 1
    final int familyID = PreambleUtil.extractFamilyID(pre0);         //Byte 2
    this.lgMaxMapSize = PreambleUtil.extractLgMaxMapSize(pre0); //Byte 3
    final boolean empty = (PreambleUtil.extractFlags(pre0) & PreambleUtil.EMPTY_FLAG_MASK) != 0; //Byte 5

    // Checks
    final boolean preLongsEq1 = (preLongs == 1);        //Byte 0
    final boolean preLongsEqMax = (preLongs == maxPreLongs);
    if (!preLongsEq1 && !preLongsEqMax) {
      throw new SketchesArgumentException(
          "Possible Corruption: PreLongs must be 1 or " + maxPreLongs + ": " + preLongs);
    }
    if (serVer != PreambleUtil.SER_VER) {                            //Byte 1
      throw new SketchesArgumentException(
          "Possible Corruption: Ser Ver must be " + PreambleUtil.SER_VER + ": " + serVer);
    }
    final int actFamID = Family.FREQUENCY.getID();      //Byte 2
    if (familyID != actFamID) {
      throw new SketchesArgumentException(
          "Possible Corruption: FamilyID must be " + actFamID + ": " + familyID);
    }
    if (empty ^ preLongsEq1) {                          //Byte 5 and Byte 0
      throw new SketchesArgumentException(
          "Possible Corruption: (PreLongs == 1) ^ Empty == True.");
    }

    if (!empty) {
      // Sketch has data! point to read-only data
      final int activeItems = PreambleUtil.extractActiveItems(mem.getLong(Long.BYTES));
      this.directHashMap = new DirectReversePurgeLongHashMap(1 << lgMaxMapSize, activeItems, mem.region(Long.BYTES * preLongs, activeItems * (DirectReversePurgeLongHashMap.VALUE_SIZE + DirectReversePurgeLongHashMap.KEY_SIZE)));

      this.streamLength = mem.getLong(Long.BYTES * 2);
      this.offset = mem.getLong(Long.BYTES * 3);
      final int maxMapCap =
          (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
      this.sampleSize = Math.min(Util.SAMPLE_SIZE, maxMapCap);
    } else {
      this.directHashMap = new DirectReversePurgeLongHashMap(1 << Util.LG_MIN_MAP_SIZE, 0, null);
    }
  }

  /**
   * @return A longs sketch that wraps writable memory wmem as the backing storage, wmem is cleared
   * in the constructor
   */
  public LongsSketchWrap(final int maxMapSize, final WritableMemory wmem)
  {
    wmem.clear();
    this.isDirectHashMap = true;
    this.directHashMap = new DirectReversePurgeLongHashMap(maxMapSize, wmem);
    this.lgMaxMapSize = Math.max(com.yahoo.sketches.Util.toLog2(maxMapSize, "maxMapSize"), Util.LG_MIN_MAP_SIZE);
    this.curMapCap = directHashMap.getCapacity();
    final int maxMapCap =
        (int) ((1 << this.lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
    this.offset = 0;
    this.sampleSize = Math.min(Util.SAMPLE_SIZE, maxMapCap);
  }

  /**
   * Returns a byte array representation of this sketch
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray()
  {
    // If in read-only mode, we already have the snapshot of the serialization format
    if (this.mem != null) {
      final byte[] outArr = new byte[(int) this.mem.getCapacity()];
      this.mem.getByteArray(0, outArr, 0, outArr.length);
      return outArr;
    }

    final int preLongs, outBytes;
    final boolean empty = isEmpty();
    final int activeItems = getNumActiveItems();
    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.FREQUENCY.getMaxPreLongs();
      outBytes = (preLongs + 2 * activeItems) << 3; //2 because both keys and values are longs
    }

    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build first preLong empty or not
    long pre0 = 0L;
    pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                  //Byte 0
    pre0 = PreambleUtil.insertSerVer(PreambleUtil.SER_VER, pre0);                     //Byte 1
    pre0 = PreambleUtil.insertFamilyID(Family.FREQUENCY.getID(), pre0);  //Byte 2
    pre0 = PreambleUtil.insertLgMaxMapSize(lgMaxMapSize, pre0);          //Byte 3
    pre0 = PreambleUtil.insertLgCurMapSize(isDirectHashMap ? directHashMap.getLgLength() : hashMap.getLgLength(), pre0); //Byte 4
    pre0 = (empty) ? PreambleUtil.insertFlags(PreambleUtil.EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); //Byte 5

    if (empty) {
      mem.putLong(0, pre0);
    } else {
      final long pre = 0;
      final long[] preArr = new long[preLongs];
      preArr[0] = pre0;
      preArr[1] = PreambleUtil.insertActiveItems(activeItems, pre);
      preArr[2] = this.streamLength;
      preArr[3] = this.offset;
      mem.putLongArray(0, preArr, 0, preLongs);
      final int preBytes = preLongs << 3;
      mem.putLongArray(preBytes, isDirectHashMap ? directHashMap.getActiveValues() : hashMap.getActiveValues(), 0, activeItems);

      mem.putLongArray(preBytes + (activeItems << 3), isDirectHashMap ? directHashMap.getActiveKeys() : hashMap.getActiveKeys(), 0,
          activeItems);
    }

    return outArr;
  }

  /**
   * Update this sketch with an item and a frequency count of one.
   * @param item for which the frequency should be increased.
   */
  public void update(final long item)
  {
    update(item, 1);
  }

  /**
   * Update this sketch with a item and a positive frequency count (or weight).
   * @param item for which the frequency should be increased. The item can be any long value
   * and is only used by the sketch to determine uniqueness.
   * @param count the amount by which the frequency of the item should be increased.
   * An count of zero is a no-op, and a negative count will throw an exception.
   */
  public void update(final long item, final long count)
  {
    if (count == 0) {
      return;
    }
    if (count < 0) {
      throw new SketchesArgumentException("Count may not be negative");
    }
    this.streamLength += count;
    if (isDirectHashMap) {
      directHashMap.adjustOrPutValue(item, count);
    } else {
      hashMap.adjustOrPutValue(item, count);
    }

    if (getNumActiveItems() > curMapCap) { //over the threshold, we need to do something
      if (!isDirectHashMap && hashMap.getLgLength() < lgMaxMapSize) { //below tgt size, we can grow
        hashMap.resize(2 * hashMap.getLength());
        curMapCap = hashMap.getCapacity();
      } else { //At tgt size, must purge
        if (isDirectHashMap) {
          offset += directHashMap.purge(sampleSize);
        } else {
          offset += hashMap.purge(sampleSize);
        }

        if (getNumActiveItems() > getMaximumMapCapacity()) {
          throw new SketchesStateException("Purge did not reduce active items.");
        }
      }
    }
  }

  /**
   * This function merges the other sketch into this one.
   * The other sketch may be of a different size.
   *
   * @param other sketch of this class
   * @return a sketch whose estimates are within the guarantees of the
   * largest error tolerance of the two merged sketches.
   */
  public LongsSketchWrap merge(final LongsSketchWrap other)
  {
    if (other == null || other.isEmpty()) {
      return this;
    }

    final long streamLen = this.streamLength + other.streamLength; //capture before merge

    if (other.isDirectHashMap) {
      final DirectReversePurgeLongHashMap.Iterator iter = other.directHashMap.iterator();
      while (iter.next()) { //this may add to offset during rebuilds
        this.update(iter.getKey(), iter.getValue());
      }
    } else {
      final ReversePurgeLongHashMap.Iterator iter = other.hashMap.iterator();
      while (iter.next()) { //this may add to offset during rebuilds
        this.update(iter.getKey(), iter.getValue());
      }
    }
    this.offset += other.offset;
    this.streamLength = streamLen; //corrected streamLength
    return this;
  }

  /**
   * Gets the estimate of the frequency of the given item.
   * Note: The true frequency of a item would be the sum of the counts as a result of the
   * two update functions.
   *
   * @param item the given item
   * @return the estimate of the frequency of the given item
   */
  public long getEstimate(final long item)
  {
    // If item is tracked:
    // Estimate = itemCount + offset; Otherwise it is 0.
    final long itemCount = isDirectHashMap ? directHashMap.get(item) : hashMap.get(item);
    return (itemCount > 0) ? itemCount + offset : 0;
  }

  /**
   * Use this method if we already know the count associated with an item
   */
  private long getEstimateWithItemCount(final long itemCount)
  {
    return itemCount + offset;
  }

  /**
   * Gets the guaranteed upper bound frequency of the given item.
   *
   * @param item the given item
   * @return the guaranteed upper bound frequency of the given item. That is, a number which
   * is guaranteed to be no smaller than the real frequency.
   */
  public long getUpperBound(final long item)
  {
    // UB = itemCount + offset
    return (isDirectHashMap ? directHashMap.get(item) : hashMap.get(item)) + offset;
  }

  /**
   * Use this method if we already know the count associated with an item
   */
  private long getUpperBoundWithItemCount(final long itemCount)
  {
    return itemCount + offset;
  }

  /**
   * Gets the guaranteed lower bound frequency of the given item, which can never be
   * negative.
   *
   * @param item the given item.
   * @return the guaranteed lower bound frequency of the given item. That is, a number which
   * is guaranteed to be no larger than the real frequency.
   */
  public long getLowerBound(final long item)
  {
    //LB = itemCount or 0
    return isDirectHashMap ? directHashMap.get(item) : hashMap.get(item);
  }

  /**
   * Use this method if we already know the count associated with an item
   */
  private long getLowerBoundWithItemCount(final long itemCount)
  {
    return itemCount;
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given a threshold and an ErrorCondition. If the threshold is lower than getMaximumError(),
   * then getMaximumError() will be used instead.
   *
   * <p>The method first examines all active items in the sketch (items that have a counter).
   *
   * <p>If <i>ErrorType = NO_FALSE_NEGATIVES</i>, this will include an item in the result
   * list if getUpperBound(item) &gt; threshold.
   * There will be no false negatives, i.e., no Type II error.
   * There may be items in the set with true frequencies less than the threshold
   * (false positives).</p>
   *
   * <p>If <i>ErrorType = NO_FALSE_POSITIVES</i>, this will include an item in the result
   * list if getLowerBound(item) &gt; threshold.
   * There will be no false positives, i.e., no Type I error.
   * There may be items omitted from the set with true frequencies greater than the
   * threshold (false negatives). This is a subset of the NO_FALSE_NEGATIVES case.</p>
   *
   * @param threshold to include items in the result list
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public Row[] getFrequentItems(final long threshold, final ErrorType errorType)
  {
    return sortItems(threshold > getMaximumError() ? threshold : getMaximumError(), errorType);
  }

  /**
   * Returns an array of Rows that include frequent items, estimates, upper and lower bounds
   * given an ErrorCondition and the default threshold.
   * This is the same as getFrequentItems(getMaximumError(), errorType)
   *
   * @param errorType determines whether no false positives or no false negatives are
   * desired.
   * @return an array of frequent items
   */
  public Row[] getFrequentItems(final ErrorType errorType)
  {
    return sortItems(getMaximumError(), errorType);
  }

  /**
   * Resets this sketch to a virgin state.
   */
  public void reset()
  {
    if (isDirectHashMap) {
      throw new UnsupportedOperationException("Not implemented");
    } else {
      this.hashMap = new ReversePurgeLongHashMap(1 << Util.LG_MIN_MAP_SIZE);
      this.curMapCap = hashMap.getCapacity();
      this.offset = 0;
      this.streamLength = 0;
    }
  }

  /**
   * Row class that defines the return values from a getFrequentItems query.
   */
  public static class Row implements Comparable<Row>
  {
    final long item;
    final long est;
    final long ub;
    final long lb;
    private static final String FMT = ("  %20d%20d%20d %d");
    private static final String HFMT = ("  %20s%20s%20s %s");
    private static final String JSON_FMT = "{\"Est\":%d,\"UB\":%d,\"LB\":%d,\"Item\":%d}";

    Row(final long item, final long estimate, final long ub, final long lb)
    {
      this.item = item;
      this.est = estimate;
      this.ub = ub;
      this.lb = lb;
    }

    /**
     * @return item of type T
     */
    public long getItem()
    {
      return item;
    }

    /**
     * @return the estimate
     */
    public long getEstimate()
    {
      return est;
    }

    /**
     * @return the upper bound
     */
    public long getUpperBound()
    {
      return ub;
    }

    /**
     * @return return the lower bound
     */
    public long getLowerBound()
    {
      return lb;
    }

    /**
     * @return the descriptive row header
     */

    public String getJson()
    {
      return StringUtils.format(JSON_FMT, est, ub, lb, item);
    }

    @Override
    public String toString()
    {
      return StringUtils.format(FMT, est, ub, lb, item);
    }

    /**
     * This compareTo is strictly limited to the Row.getEstimate() value and does not imply any
     * ordering whatsoever to the other elements of the row: item and upper and lower bounds.
     * Defined this way, this compareTo will be consistent with hashCode() and equals(Object).
     * @param that the other row to compare to.
     * @return a negative integer, zero, or a positive integer as this.getEstimate() is less than,
     * equal to, or greater than that.getEstimate().
     */
    @Override
    public int compareTo(final Row that)
    {
      return Long.compare(this.est, that.est);
    }

    /**
     * This hashCode is computed only from the Row.getEstimate() value.
     * Defined this way, this hashCode will be consistent with equals(Object):<br>
     * If (x.equals(y)) implies: x.hashCode() == y.hashCode().<br>
     * If (!x.equals(y)) does NOT imply: x.hashCode() != y.hashCode().
     * @return the hashCode computed from getEstimate().
     */
    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (est ^ (est >>> 32));
      return result;
    }

    /**
     * This equals is computed only from the Row.getEstimate() value and does not imply equality
     * of the other items within the row: item and upper and lower bounds.
     * Defined this way, this equals will be consistent with compareTo(Row).
     * @param obj the other row to determine equality with.
     * @return true if this.getEstimate() equals ((Row)obj).getEstimate().
     */
    @Override
    public boolean equals(final Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof Row)) {
        return false;
      }
      final Row that = (Row) obj;
      if (est != that.est) {
        return false;
      }
      return true;
    }

  } // End of class Row

  Row[] sortItems(final long threshold, final ErrorType errorType)
  {
    final ArrayList<Row> rowList = new ArrayList<Row>();

    if (isDirectHashMap) {
      final DirectReversePurgeLongHashMap.Iterator iter = directHashMap.iterator();
      if (errorType == ErrorType.NO_FALSE_NEGATIVES) {
        while (iter.next()) {
          final long est = mem != null ? getEstimateWithItemCount(iter.getValue()) : getEstimate(iter.getKey());
          final long ub = mem != null ? getUpperBoundWithItemCount(iter.getValue()) : getUpperBound(iter.getKey());
          final long lb = mem != null ? getLowerBoundWithItemCount(iter.getValue()) : getLowerBound(iter.getKey());
          if (ub >= threshold) {
            final Row row = new Row(iter.getKey(), est, ub, lb);
            rowList.add(row);
          }
        }
      } else { //NO_FALSE_POSITIVES
        while (iter.next()) {
          final long est = mem != null ? getEstimateWithItemCount(iter.getValue()) : getEstimate(iter.getKey());
          final long ub = mem != null ? getUpperBoundWithItemCount(iter.getValue()) : getUpperBound(iter.getKey());
          final long lb = mem != null ? getLowerBoundWithItemCount(iter.getValue()) : getLowerBound(iter.getKey());
          if (lb >= threshold) {
            final Row row = new Row(iter.getKey(), est, ub, lb);
            rowList.add(row);
          }
        }
      }
    } else {
      final ReversePurgeLongHashMap.Iterator iter = hashMap.iterator();
      if (errorType == ErrorType.NO_FALSE_NEGATIVES) {
        while (iter.next()) {
          final long est = getEstimate(iter.getKey());
          final long ub = getUpperBound(iter.getKey());
          final long lb = getLowerBound(iter.getKey());
          if (ub >= threshold) {
            final Row row = new Row(iter.getKey(), est, ub, lb);
            rowList.add(row);
          }
        }
      } else { //NO_FALSE_POSITIVES
        while (iter.next()) {
          final long est = getEstimate(iter.getKey());
          final long ub = getUpperBound(iter.getKey());
          final long lb = getLowerBound(iter.getKey());
          if (lb >= threshold) {
            final Row row = new Row(iter.getKey(), est, ub, lb);
            rowList.add(row);
          }
        }
      }
    }

    // descending order
    rowList.sort(new Comparator<Row>() {
      @Override
      public int compare(final Row r1, final Row r2)
      {
        return r2.compareTo(r1);
      }
    });

    final Row[] rowsArr = rowList.toArray(new Row[0]);
    return rowsArr;
  }

  /**
   * Returns the current number of counters the sketch is configured to support.
   *
   * @return the current number of counters the sketch is configured to support.
   */
  public int getCurrentMapCapacity()
  {
    return this.curMapCap;
  }

  /**
   * @return An upper bound on the maximum error of getEstimate(item) for any item.
   * This is equivalent to the maximum distance between the upper bound and the lower bound
   * for any item.
   */
  public long getMaximumError()
  {
    return offset;
  }

  /**
   * Returns true if this sketch is empty
   *
   * @return true if this sketch is empty
   */
  public boolean isEmpty()
  {
    return getNumActiveItems() == 0;
  }

  /**
   * Returns the sum of the frequencies (weights or counts) in the stream seen so far by the sketch
   *
   * @return the sum of the frequencies in the stream seen so far by the sketch
   */
  public long getStreamLength()
  {
    return this.streamLength;
  }

  /**
   * Returns the maximum number of counters the sketch is configured to support.
   *
   * @return the maximum number of counters the sketch is configured to support.
   */
  public int getMaximumMapCapacity()
  {
    return (int) ((1 << lgMaxMapSize) * ReversePurgeLongHashMap.getLoadFactor());
  }

  public int getMaxMapSize()
  {
    return 1 << lgMaxMapSize;
  }

  /**
   * @return the number of active items in the sketch.
   */
  public int getNumActiveItems()
  {
    return isDirectHashMap ? directHashMap.getNumActive() : hashMap.getNumActive();
  }

  /**
   * Returns max number of bytes this sketch will use
   */
  public static int getMaxStorageBytes(int maxMapSize)
  {
    return DirectReversePurgeLongHashMap.getMaxStorageBytes(maxMapSize);
  }

  /**
   * Returns a human readable summary of this sketch.
   * @return a human readable summary of this sketch.
   */
  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("FrequentLongsSketch:").append(com.yahoo.sketches.Util.LS);
    sb.append("  Stream Length    : " + streamLength).append(com.yahoo.sketches.Util.LS);
    sb.append("  Max Error Offset : " + offset).append(com.yahoo.sketches.Util.LS);
    sb.append((isDirectHashMap ? directHashMap : hashMap));
    return sb.toString();
  }
}
