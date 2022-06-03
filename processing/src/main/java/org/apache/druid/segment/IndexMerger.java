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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.ImplementedBy;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@ImplementedBy(IndexMergerV9.class)
public interface IndexMerger
{
  Logger log = new Logger(IndexMerger.class);

  SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
  int INVALID_ROW = -1;
  int UNLIMITED_MAX_COLUMNS_TO_MERGE = -1;

  static List<String> getMergedDimensionsFromQueryableIndexes(
      List<QueryableIndex> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    return getMergedDimensions(toIndexableAdapters(indexes), dimensionsSpec);
  }

  static List<IndexableAdapter> toIndexableAdapters(List<QueryableIndex> indexes)
  {
    return indexes.stream().map(QueryableIndexIndexableAdapter::new).collect(Collectors.toList());
  }

  static List<String> getMergedDimensions(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    if (indexes.size() == 0) {
      return ImmutableList.of();
    }
    List<String> commonDimOrder = getLongestSharedDimOrder(indexes, dimensionsSpec);
    if (commonDimOrder == null) {
      log.warn("Indexes have incompatible dimension orders and there is no valid dimension ordering"
               + " in the ingestionSpec, using lexicographic order.");
      return getLexicographicMergedDimensions(indexes);
    } else {
      return commonDimOrder;
    }
  }

  @Nullable
  static List<String> getLongestSharedDimOrder(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    int maxSize = 0;
    Iterable<String> orderingCandidate = null;
    for (IndexableAdapter index : indexes) {
      int iterSize = index.getDimensionNames().size();
      if (iterSize > maxSize) {
        maxSize = iterSize;
        orderingCandidate = index.getDimensionNames();
      }
    }

    if (orderingCandidate == null) {
      return null;
    }

    if (isDimensionOrderingValid(indexes, orderingCandidate)) {
      return ImmutableList.copyOf(orderingCandidate);
    } else {
      log.info("Indexes have incompatible dimension orders, try falling back on dimension ordering from ingestionSpec");
      // Check if there is a valid dimension ordering in the ingestionSpec to fall back on
      if (dimensionsSpec == null || CollectionUtils.isNullOrEmpty(dimensionsSpec.getDimensionNames())) {
        log.info("Cannot fall back on dimension ordering from ingestionSpec as it does not exist");
        return null;
      }
      List<String> candidate = new ArrayList<>(dimensionsSpec.getDimensionNames());
      // Remove all dimensions that does not exist within the indexes from the candidate
      Set<String> allValidDimensions = indexes.stream()
                                         .flatMap(indexableAdapter -> indexableAdapter.getDimensionNames().stream())
                                         .collect(Collectors.toSet());
      candidate.retainAll(allValidDimensions);
      // Sanity check that there is no extra/missing columns
      if (candidate.size() != allValidDimensions.size()) {
        log.error("Dimension mismatched between ingestionSpec and indexes. ingestionSpec[%s] indexes[%s]",
                  candidate,
                  allValidDimensions);
        return null;
      }

      // Sanity check that all indexes dimension ordering is the same as the ordering in candidate
      if (!isDimensionOrderingValid(indexes, candidate)) {
        log.error("Dimension from ingestionSpec has invalid ordering");
        return null;
      }
      log.info("Dimension ordering from ingestionSpec is valid. Fall back on dimension ordering [%s]", candidate);
      return candidate;
    }
  }

  static boolean isDimensionOrderingValid(List<IndexableAdapter> indexes, Iterable<String> orderingCandidate)
  {
    for (IndexableAdapter index : indexes) {
      Iterator<String> candidateIter = orderingCandidate.iterator();
      for (String matchDim : index.getDimensionNames()) {
        boolean matched = false;
        while (candidateIter.hasNext()) {
          String nextDim = candidateIter.next();
          if (matchDim.equals(nextDim)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      }
    }
    return true;
  }

  static List<String> getLexicographicMergedDimensions(List<IndexableAdapter> indexes)
  {
    return mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
  }

  static <T extends Comparable<? super T>> ArrayList<T> mergeIndexed(List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = new TreeSet<>(Comparators.naturalNullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  /**
   * @return True if at least one dimension in the schema has supplimental index (for now, supplimental index only
   * consists of bloom filter index); false otherwise
   */
  static boolean hasSupplimentalIndex(@Nullable DimensionsSpec dimensionsSpec)
  {
    if (dimensionsSpec == null) {
      return false;
    }
    return hasBloomFilterIndexes(dimensionsSpec);
  }

  static boolean hasBloomFilterIndexes(@Nullable DimensionsSpec dimensionsSpec)
  {
    if (dimensionsSpec == null) {
      return false;
    }
    return !getDimensionNamesHasBloomFilterIndexes(dimensionsSpec).isEmpty();
  }

  static List<String> getDimensionNamesHasBloomFilterIndexes(@Nullable DimensionsSpec dimensionsSpec)
  {
    if (dimensionsSpec == null) {
      return Collections.emptyList();
    }
    return dimensionsSpec.getDimensions()
                         .stream()
                         .filter(DimensionSchema::hasBloomFilterIndexes)
                         .map(DimensionSchema::getName)
                         .collect(Collectors.toList());
  }

  /**
   * Set hasBloomFilterIndexes in ColumnCapabilities for all provided dimension names
   */
  static void setHasBloomFilterIndexesInColumnCapabilities(List<String> dimensionNamesHasBloomFilterIndexes,
                                                           Function<String, ColumnCapabilities> getColumnCapabilities)
  {
    for (String d : dimensionNamesHasBloomFilterIndexes) {
      ColumnCapabilities columnCapabilities = getColumnCapabilities.apply(d);
      if (columnCapabilities == null) {
        continue;
      }
      if (columnCapabilities instanceof ColumnCapabilitiesImpl) {
        ((ColumnCapabilitiesImpl) columnCapabilities).setHasBloomFilterIndexes(true);
      } else {
        throw new ISE("Unsupported ColumnCapabilities implementation");
      }
    }
  }

  default File persist(
      IncrementalIndex index,
      File indexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return Objects.requireNonNull(persist(index, indexOutDir, null, indexSpec, segmentWriteOutMediumFactory).lhs);
  }

  Pair<File, File> persist(
      IncrementalIndex index,
      File indexOutDir,
      @Nullable File supplimentalIndexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  default File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File indexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return Objects.requireNonNull(persist(
        index,
        dataInterval,
        indexOutDir,
        null,
        indexSpec,
        segmentWriteOutMediumFactory
    ).lhs);
  }

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param indexOutDir  the directory to persist the index data to
   * @param supplimentalIndexOutDir the directory to persist the supplimental index data to
   *
   * @return A pair with the left being the index output directory and the right being the supplimental index output
   * directory
   *
   * @throws IOException if an IO error occurs persisting the index
   */
  Pair<File, File> persist(
      IncrementalIndex index,
      Interval dataInterval,
      File indexOutDir,
      @Nullable File supplimentalIndexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  default File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File indexOutDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return Objects.requireNonNull(persist(
        index,
        dataInterval,
        indexOutDir,
        null,
        indexSpec,
        progress,
        segmentWriteOutMediumFactory
    ).lhs);
  }

  Pair<File, File> persist(
      IncrementalIndex index,
      Interval dataInterval,
      File indexOutDir,
      @Nullable File supplimentalIndexOutDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  Pair<File, File> mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File indexOutDir,
      @Nullable File supplimentalIndexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  Pair<File, File> mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File indexOutDir,
      @Nullable File supplimentalIndexOutDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  default File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File indexOutDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return Objects.requireNonNull(mergeQueryableIndex(
        indexes,
        rollup,
        metricAggs,
        indexOutDir,
        null,
        indexSpec,
        segmentWriteOutMediumFactory
    ).lhs);
  }

  default File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File indexOutDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return Objects.requireNonNull(mergeQueryableIndex(
        indexes,
        rollup,
        metricAggs,
        indexOutDir,
        null,
        indexSpec,
        progress,
        segmentWriteOutMediumFactory
    ).lhs);
  }

  /**
   * Merge a collection of {@link QueryableIndex}.
   *
   * Only used as a convenience method in tests. In production code, use the full version
   * {@link #mergeQueryableIndex(List, boolean, AggregatorFactory[], DimensionsSpec, File, IndexSpec, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory, int)}.
   */
  @VisibleForTesting
  default File mergeQueryableIndex(
          List<QueryableIndex> indexes,
          boolean rollup,
          AggregatorFactory[] metricAggs,
          File outDir,
          IndexSpec indexSpec,
          @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
          int maxColumnsToMerge
  ) throws IOException
  {
    return mergeQueryableIndex(
            indexes,
            rollup,
            metricAggs,
            null,
            outDir,
            indexSpec,
            indexSpec,
            new BaseProgressIndicator(),
            segmentWriteOutMediumFactory,
            maxColumnsToMerge
    );
  }

  /**
   * Merge a collection of {@link QueryableIndex}.
   */

  File mergeQueryableIndex(
          List<QueryableIndex> indexes,
          boolean rollup,
          AggregatorFactory[] metricAggs,
          @Nullable DimensionsSpec dimensionsSpec,
          File outDir,
          IndexSpec indexSpec,
          IndexSpec indexSpecForIntermediatePersists,
          ProgressIndicator progress,
          @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
          int maxColumnsToMerge
  ) throws IOException;

  Pair<File, File> mergeQueryableIndex(
          List<QueryableIndex> indexes,
          boolean rollup,
          AggregatorFactory[] metricAggs,
          @Nullable DimensionsSpec dimensionsSpec,
          File outDir,
          @Nullable File supplimentalIndexDir,
          IndexSpec indexSpec,
          IndexSpec indexSpecForIntermediatePersists,
          ProgressIndicator progress,
          @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
          int maxColumnsToMerge
  ) throws IOException;
  /**
   * Only used as a convenience method in tests.
   *
   * In production code, to merge multiple {@link QueryableIndex}, use
   * {@-link #mergeQueryableIndex(List, boolean, AggregatorFactory[], DimensionsSpec, File, IndexSpec, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory, int)}.
   * To merge multiple {@link IncrementalIndex}, call one of the {@link #persist} methods and then merge the resulting
   * {@link QueryableIndex}.
   */
  @VisibleForTesting
  File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int maxColumnsToMerge
  ) throws IOException;

  /**
   * This method applies {@link DimensionMerger#convertSortedSegmentRowValuesToMergedRowValues(int, ColumnValueSelector)} to
   * all dimension column selectors of the given sourceRowIterator, using the given index number.
   */
  static TransformableRowIterator toMergedIndexRowIterator(
      TransformableRowIterator sourceRowIterator,
      int indexNumber,
      final List<DimensionMergerV9> mergers
  )
  {
    RowPointer sourceRowPointer = sourceRowIterator.getPointer();
    TimeAndDimsPointer markedSourceRowPointer = sourceRowIterator.getMarkedPointer();
    boolean anySelectorChanged = false;
    ColumnValueSelector[] convertedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    ColumnValueSelector[] convertedMarkedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    for (int i = 0; i < mergers.size(); i++) {
      ColumnValueSelector sourceDimensionSelector = sourceRowPointer.getDimensionSelector(i);
      ColumnValueSelector convertedDimensionSelector =
          mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(indexNumber, sourceDimensionSelector);
      convertedDimensionSelectors[i] = convertedDimensionSelector;
      // convertedDimensionSelector could be just the same object as sourceDimensionSelector, it means that this
      // type of column doesn't have any kind of special per-index encoding that needs to be converted to the "global"
      // encoding. E. g. it's always true for subclasses of NumericDimensionMergerV9.
      //noinspection ObjectEquality
      anySelectorChanged |= convertedDimensionSelector != sourceDimensionSelector;

      convertedMarkedDimensionSelectors[i] = mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(
          indexNumber,
          markedSourceRowPointer.getDimensionSelector(i)
      );
    }
    // If none dimensions are actually converted, don't need to transform the sourceRowIterator, adding extra
    // indirection layer. It could be just returned back from this method.
    if (!anySelectorChanged) {
      return sourceRowIterator;
    }
    return makeRowIteratorWithConvertedDimensionColumns(
        sourceRowIterator,
        convertedDimensionSelectors,
        convertedMarkedDimensionSelectors
    );
  }

  static TransformableRowIterator makeRowIteratorWithConvertedDimensionColumns(
      TransformableRowIterator sourceRowIterator,
      ColumnValueSelector[] convertedDimensionSelectors,
      ColumnValueSelector[] convertedMarkedDimensionSelectors
  )
  {
    RowPointer convertedRowPointer = sourceRowIterator.getPointer().withDimensionSelectors(convertedDimensionSelectors);
    TimeAndDimsPointer convertedMarkedRowPointer =
        sourceRowIterator.getMarkedPointer().withDimensionSelectors(convertedMarkedDimensionSelectors);
    return new ForwardingRowIterator(sourceRowIterator)
    {
      @Override
      public RowPointer getPointer()
      {
        return convertedRowPointer;
      }

      @Override
      public TimeAndDimsPointer getMarkedPointer()
      {
        return convertedMarkedRowPointer;
      }
    };
  }
}
