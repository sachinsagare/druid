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

package org.apache.druid.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.NamespacedVersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * This class is responsible for managing data sources and their states like timeline, total segment size, and number of
 * segments.  All public methods of this class must be thread-safe.
 */
public class SegmentManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentManager.class);

  private final SegmentLoader segmentLoader;
  private final ConcurrentHashMap<String, DataSourceState> dataSources = new ConcurrentHashMap<>();

  /**
   * Represent the state of a data source including the timeline, total segment size, and number of segments.
   */
  public static class DataSourceState
  {
    private final NamespacedVersionedIntervalTimeline<String, ReferenceCountingSegment> timeline =
        new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
    private long totalSegmentSize;
    private long numSegments;

    private void addSegment(DataSegment segment)
    {
      totalSegmentSize += segment.getSize();
      numSegments++;
    }

    private void removeSegment(DataSegment segment)
    {
      totalSegmentSize -= segment.getSize();
      numSegments--;
    }

    public NamespacedVersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline()
    {
      return timeline;
    }

    public long getTotalSegmentSize()
    {
      return totalSegmentSize;
    }

    public long getNumSegments()
    {
      return numSegments;
    }

    public boolean isEmpty()
    {
      return numSegments == 0;
    }
  }

  @Inject
  public SegmentManager(
      SegmentLoader segmentLoader
  )
  {
    this.segmentLoader = segmentLoader;
  }

  @VisibleForTesting
  Map<String, DataSourceState> getDataSources()
  {
    return dataSources;
  }

  /**
   * Returns a map of dataSource to the total byte size of segments managed by this segmentManager.  This method should
   * be used carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and their total byte sizes
   */
  public Map<String, Long> getDataSourceSizes()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getTotalSegmentSize);
  }

  /**
   * Returns a map of dataSource to the number of segments managed by this segmentManager.  This method should be
   * carefully because the returned map might be different from the actual data source states.
   *
   * @return a map of dataSources and number of segments
   */
  public Map<String, Long> getDataSourceCounts()
  {
    return CollectionUtils.mapValues(dataSources, SegmentManager.DataSourceState::getNumSegments);
  }

  public boolean isSegmentCached(final DataSegment segment)
  {
    return segmentLoader.isSegmentLoaded(segment);
  }

  @Nullable
  public NamespacedVersionedIntervalTimeline<String, ReferenceCountingSegment> getTimeline(String dataSource)
  {
    final DataSourceState dataSourceState = dataSources.get(dataSource);
    return dataSourceState == null ? null : dataSourceState.getTimeline();
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   *
   * @param loadSegmentIntoPageCacheExec This thread pool is optional. Usually you can give one with
   *                                     more capacity on bootstrap. If null is specified, the
   *                                     thread pool to load segments into page cache on download
   *                                     will be used.
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment, ExecutorService loadSegmentIntoPageCacheExec) throws SegmentLoadingException
  {
    final Segment adapter = getAdapter(segment);

    final SettableSupplier<Boolean> resultSupplier = new SettableSupplier<>();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        segment.getDataSource(),
        (k, v) -> {
          final DataSourceState dataSourceState = v == null ? new DataSourceState() : v;
          final NamespacedVersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
              dataSourceState.getTimeline();
          final PartitionHolder<ReferenceCountingSegment> entry = loadedIntervals.findEntry(
              segment.getInterval(),
              segment.getVersion()
          );

          if ((entry != null) && (entry.getChunk(segment.getShardSpec().getPartitionNum()) != null)) {
            log.warn("Told to load an adapter for segment[%s] that already exists", segment.getId());
            resultSupplier.set(false);
          } else {
            loadedIntervals.add(
                NamespacedVersionedIntervalTimeline.getNamespace(segment.getShardSpec().getIdentifier()),
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(
                    ReferenceCountingSegment.wrapSegment(adapter, segment.getShardSpec())
                )
            );
            dataSourceState.addSegment(segment);
            segmentLoader.loadSegmentIntoPageCache(segment, loadSegmentIntoPageCacheExec);
            resultSupplier.set(true);
          }
          return dataSourceState;
        }
    );

    return resultSupplier.get();
  }

  /**
   * Load a single segment.
   *
   * @param segment segment to load
   *
   * @return true if the segment was newly loaded, false if it was already loaded
   *
   * @throws SegmentLoadingException if the segment cannot be loaded
   */
  public boolean loadSegment(final DataSegment segment) throws SegmentLoadingException
  {
    return loadSegment(segment, null);
  }

  private Segment getAdapter(final DataSegment segment) throws SegmentLoadingException
  {
    final Segment adapter;
    try {
      adapter = segmentLoader.getSegment(segment);
    }
    catch (SegmentLoadingException e) {
      segmentLoader.cleanup(segment);
      throw e;
    }

    if (adapter == null) {
      throw new SegmentLoadingException("Null adapter from loadSpec[%s]", segment.getLoadSpec());
    }
    return adapter;
  }

  public void dropSegment(final DataSegment segment)
  {
    final String dataSource = segment.getDataSource();

    // compute() is used to ensure that the operation for a data source is executed atomically
    dataSources.compute(
        dataSource,
        (dataSourceName, dataSourceState) -> {
          if (dataSourceState == null) {
            log.info("Told to delete a queryable for a dataSource[%s] that doesn't exist.", dataSourceName);
            return null;
          } else {
            final NamespacedVersionedIntervalTimeline<String, ReferenceCountingSegment> loadedIntervals =
                dataSourceState.getTimeline();

            final ShardSpec shardSpec = segment.getShardSpec();
            final PartitionChunk<ReferenceCountingSegment> removed = loadedIntervals.remove(
                NamespacedVersionedIntervalTimeline.getNamespace(segment.getShardSpec().getIdentifier()),
                segment.getInterval(),
                segment.getVersion(),
                // remove() internally searches for a partitionChunk to remove which is *equal* to the given
                // partitionChunk. Note that partitionChunk.equals() checks only the partitionNum, but not the object.
                segment.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(null, shardSpec))
            );
            final ReferenceCountingSegment oldQueryable = (removed == null) ? null : removed.getObject();

            if (oldQueryable != null) {
              dataSourceState.removeSegment(segment);

              log.info("Attempting to close segment %s", segment.getId());
              oldQueryable.close();
            } else {
              log.info(
                  "Told to delete a queryable on dataSource[%s] for interval[%s] and version[%s] that I don't have.",
                  dataSourceName,
                  segment.getInterval(),
                  segment.getVersion()
              );
            }

            // Returning null removes the entry of dataSource from the map
            return dataSourceState.isEmpty() ? null : dataSourceState;
          }
        }
    );

    segmentLoader.cleanup(segment);
  }
}
