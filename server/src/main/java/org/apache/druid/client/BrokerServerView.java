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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.hash.BloomFilter;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.ComplementaryNamespacedVersionedIntervalTimeline;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.NamespacedVersionedIntervalTimeline;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.BloomFilterStreamFanOutHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 */
@ManageLifecycle
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, QueryableDruidServer> clients;
  private final Map<SegmentId, ServerSelector> selectors;
  private final Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> timelines;
  private final ConcurrentMap<TimelineCallback, Executor> timelineCallbacks = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final FilteredServerInventoryView baseView;
  private final TierSelectorStrategy tierSelectorStrategy;
  private final ServiceEmitter emitter;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> segmentFilter;
  private final Map<String, List<String>> dataSourceComplementaryMapToQueryOrder;
  private final DruidProcessingConfig processingConfig;
  private final Set<String> lifetimeDataSource;
  private final ObjectMapper jsonMapper;

  private final CountDownLatch initialized = new CountDownLatch(1);
  private final ExecutorService loadSegmentSupplimentalIndexIntoShardSpecExec;

  @Inject
  public BrokerServerView(
      final QueryToolChestWarehouse warehouse,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient,
      final FilteredServerInventoryView baseView,
      final TierSelectorStrategy tierSelectorStrategy,
      final ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final BrokerDataSourceComplementConfig dataSourceComplementConfig,
      final BrokerDataSourceMultiComplementConfig dataSourceMultiComplementConfig,
      final DruidProcessingConfig processingConfig,
      final BrokerDataSourceLifetimeConfig lifetimeConfig,
      final ObjectMapper jsonMapper)
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.baseView = baseView;
    this.tierSelectorStrategy = tierSelectorStrategy;
    this.emitter = emitter;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.processingConfig = processingConfig;
    this.jsonMapper = jsonMapper;

    // TODO (lucilla) should be removed after we fully migrate to using BrokerDataSourceMultiComplementConfig
    this.dataSourceComplementaryMapToQueryOrder = CollectionUtils.mapValues(dataSourceComplementConfig.getMapping(), Collections::singletonList);
    Map<String, List<String>> dataSourceMultiComplementConfigMapping = dataSourceMultiComplementConfig.getMapping();
    dataSourceMultiComplementConfigMapping.keySet().forEach(key ->
            dataSourceComplementaryMapToQueryOrder.putIfAbsent(key, dataSourceMultiComplementConfigMapping.get(key))
    );

    this.lifetimeDataSource = lifetimeConfig.getMapping().keySet();
    this.clients = new ConcurrentHashMap<>();
    this.selectors = new HashMap<>();
    this.timelines = new HashMap<>();

    // Initialize timelines with the dataSources that we know about from dataSourceComplementaryReverseMapToQueryOrder
    // the remaining dataSources will be added in as segments are loaded onto historicals
    for (String dataSource : dataSourceComplementaryMapToQueryOrder.keySet()) {
      timelines.putIfAbsent(dataSource, getTimeline(dataSource));
    }

    this.segmentFilter = (Pair<DruidServerMetadata, DataSegment> metadataAndSegment) -> {
      if (segmentWatcherConfig.getWatchedTiers() != null
              && !segmentWatcherConfig.getWatchedTiers().contains(metadataAndSegment.lhs.getTier())) {
        return false;
      }

      if (segmentWatcherConfig.getWatchedDataSources() != null
              && !segmentWatcherConfig.getWatchedDataSources().contains(metadataAndSegment.rhs.getDataSource())) {
        return false;
      }

      return true;
    };
    ExecutorService exec = Execs.singleThreaded("BrokerServerView-%s");

    this.loadSegmentSupplimentalIndexIntoShardSpecExec = Execs.multiThreaded(
        segmentWatcherConfig.getNumThreadsToLoadSegmentSupplimentalIndexIntoShardSpec(),
        "BrokerServerView-Load-Segment-Supplimental-Index-Into-Shard-Spec-Exec-%s");

    baseView.registerSegmentCallback(
          exec,
          new ServerView.SegmentCallback() {
            @Override
            public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
            {
              serverAddedSegment(server, segment);
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction segmentRemoved(final DruidServerMetadata server, DataSegment segment)
            {
              serverRemovedSegment(server, segment);
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public CallbackAction segmentViewInitialized()
            {
              initialized.countDown();
              runTimelineCallbacks(TimelineCallback::timelineInitialized);
              return ServerView.CallbackAction.CONTINUE;
            }
          },
            segmentFilter
    );

    baseView.registerServerRemovedCallback(
        exec,
        server -> {
          removeServer(server);
          return CallbackAction.CONTINUE;
        }
    );
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (segmentWatcherConfig.isAwaitInitializationOnStart()) {
      final long startMillis = System.currentTimeMillis();
      log.info("%s waiting for initialization.", getClass().getSimpleName());
      awaitInitialization();
      log.info("%s initialized in [%,d] ms.", getClass().getSimpleName(), System.currentTimeMillis() - startMillis);
    }
  }

  public boolean isInitialized()
  {
    return initialized.getCount() == 0;
  }

  public void awaitInitialization() throws InterruptedException
  {
    initialized.await();
  }

  private QueryableDruidServer addServer(DruidServer server)
  {
    QueryableDruidServer retVal = new QueryableDruidServer<>(server, makeDirectClient(server));
    QueryableDruidServer exists = clients.put(server.getName(), retVal);
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already exists!? Well it's getting replaced", server);
    }

    return retVal;
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(
            warehouse,
            queryWatcher,
            smileMapper,
            httpClient,
            server.getScheme(),
            server.getHost(),
            emitter,
            skipDataOnException(server));
  }

  private boolean skipDataOnException(DruidServer server)
  {
    return processingConfig.skipRealtimeDataOnException() && server.getType() == ServerType.INDEXER_EXECUTOR;
  }

  private QueryableDruidServer removeServer(DruidServer server)
  {
    for (DataSegment segment : server.iterateAllSegments()) {
      serverRemovedSegment(server.getMetadata(), segment);
    }
    return clients.remove(server.getName());
  }

  @VisibleForTesting
  protected void serverAddedSegment(final DruidServerMetadata server, final DataSegment segment)
  {
    SegmentId segmentId = segment.getId();

    // Load bloom filter if the server is a historical. The bloom filter is not finalized until the segment is finalized
    // and handed off to historicals by real time tasks so we can skip if server is a real time server
    if (segment.getShardSpec() instanceof BloomFilterStreamFanOutHashBasedNumberedShardSpec &&
        server.getType().equals(ServerType.HISTORICAL)) {
      // It's possible multiple replicas in historicals have been assigned for the segment, which means we potentially
      // have loaded the bloom filter for the segment before when adding a replica for another historical. Because the
      // bloom filter doesn't change once set, we can avoid downloading from scratch
      boolean needsDownload = false;
      synchronized (lock) {
        ServerSelector selector = selectors.get(segmentId);
        if (selector != null) {
          BloomFilter<byte[]> bloomFilter = ((BloomFilterStreamFanOutHashBasedNumberedShardSpec) selector.getSegment()
                                                                                                        .getShardSpec())
              .getBloomFilter();
          if (bloomFilter != null) {
            ((BloomFilterStreamFanOutHashBasedNumberedShardSpec) segment.getShardSpec()).setBloomFilter(bloomFilter);
            log.info("Reused existing bloom filter for segment[%s] for server[%s]", segment, server);
          } else {
            needsDownload = true;
          }
        } else {
          needsDownload = true;
        }
      }

      if (needsDownload) {
        // Download and load bloom filter into shard spec asyncly in a thread pool
        loadSegmentSupplimentalIndexIntoShardSpecExec.submit(() -> {
          log.info("Adding bloom filter for segment[%s] for server[%s]", segment, server);
          final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
          File tmpDir = Files.createTempDir();
          try {
            loadSpec.loadSupplimentalIndexFile(
                tmpDir,
                BloomFilterStreamFanOutHashBasedNumberedShardSpec.BLOOM_FILTER_DIR + ".zip"
            );

            File versionFile = new File(
                tmpDir,
                BloomFilterStreamFanOutHashBasedNumberedShardSpec.BLOOM_FILTER_VERSION_FILE
            );
            byte version = (byte) (versionFile.exists() ? Ints.fromByteArray(Files.toByteArray(versionFile)) :
                                   BloomFilterStreamFanOutHashBasedNumberedShardSpec.CURRENT_VERSION_ID);
            byte[] bloomFilter = Files.toByteArray(new File(
                tmpDir,
                BloomFilterStreamFanOutHashBasedNumberedShardSpec.BLOOM_FILTER_BIN_FILE
            ));
            ((BloomFilterStreamFanOutHashBasedNumberedShardSpec) segment.getShardSpec()).deserializeBloomFilter(
                version,
                bloomFilter
            );
            log.info(
                "Loaded bloom filter of [%d] bytes for segment[%s] for server[%s]",
                bloomFilter.length,
                segment,
                server
            );
          }
          catch (SegmentLoadingException | IOException e) {
            log.error(e.getMessage());
          }
          finally {
            try {
              FileUtils.deleteDirectory(tmpDir);
            }
            catch (IOException e) {
              log.error(e.getMessage());
            }
          }
        });
      }
    }

    synchronized (lock) {
      log.debug("Adding segment[%s] for server[%s]", segment, server);

      ServerSelector selector = selectors.get(segmentId);
      if (selector == null) {
        selector = new ServerSelector(segment, tierSelectorStrategy);

        String dataSource = segment.getDataSource();
        NamespacedVersionedIntervalTimeline<String, ServerSelector> timeline = timelines
                .get(dataSource);
        if (timeline == null) {
          timeline = new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
          timelines.put(segment.getDataSource(), timeline);
        }

        timeline.add(
                NamespacedVersionedIntervalTimeline.getNamespace(
                        segment.getShardSpec().getIdentifier()),
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().createChunk(selector));
        selectors.put(segmentId, selector);
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        queryableDruidServer = addServer(baseView.getInventoryValue(server.getName()));
      }
      selector.addServerAndUpdateSegment(queryableDruidServer, segment);
      runTimelineCallbacks(callback -> callback.segmentAdded(server, segment));
    }
  }

  private void serverRemovedSegment(DruidServerMetadata server, DataSegment segment)
  {
    SegmentId segmentId = segment.getId();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
                "Asked to disassociate non-existant association between server[%s] and segment[%s]",
                server,
                segmentId
        );
      } else {
        runTimelineCallbacks(callback -> callback.serverSegmentRemoved(server, segment));
      }

      if (selector.isEmpty()) {
        NamespacedVersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
                NamespacedVersionedIntervalTimeline.getNamespace(segment.getShardSpec().getIdentifier()),
                segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
                  "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
                  segment.getInterval(),
                  segment.getVersion()
          );
        } else {
          runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
        }
      }
    }
  }


  @Nullable
  @Override
  public NamespacedVersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getNames());
    synchronized (lock) {
      return timelines.get(table);
    }
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    timelineCallbacks.put(callback, exec);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("WTF?! No QueryableDruidServer found for %s", server.getName());
        return null;
      }
      return queryableDruidServer.getQueryRunner();
    }
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    baseView.registerServerRemovedCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback, segmentFilter);
  }

  private void runTimelineCallbacks(final Function<TimelineCallback, CallbackAction> function)
  {
    for (Map.Entry<TimelineCallback, Executor> entry : timelineCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> {
            if (CallbackAction.UNREGISTER == function.apply(entry.getKey())) {
              timelineCallbacks.remove(entry.getKey());
            }
          }
      );
    }
  }

  private NamespacedVersionedIntervalTimeline<String, ServerSelector> getTimeline(String dataSource)
  {
    if (timelines.containsKey(dataSource)) {
      return timelines.get(dataSource);
    }
    NamespacedVersionedIntervalTimeline timeline;
    if (dataSourceComplementaryMapToQueryOrder.containsKey(dataSource)) {
      // We need to create a ComplementaryNamespacedVersionedIntervalTimeline
      Map<String, NamespacedVersionedIntervalTimeline<String, ServerSelector>> supportTimelinesByDataSource = new HashMap<>();
      for (String supportDataSource : dataSourceComplementaryMapToQueryOrder.get(dataSource)) {
        supportTimelinesByDataSource.putIfAbsent(supportDataSource, getTimeline(supportDataSource));
      }
      boolean islifetime = this.lifetimeDataSource.contains(dataSource);
      timeline = new ComplementaryNamespacedVersionedIntervalTimeline(
              dataSource,
              supportTimelinesByDataSource,
              dataSourceComplementaryMapToQueryOrder.get(dataSource),
              islifetime);
      timelines.put(dataSource, timeline);
    } else {
      timeline = new NamespacedVersionedIntervalTimeline<>(Ordering.natural());
      timelines.put(dataSource, timeline);
    }
    return timeline;
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return clients.values().stream()
            .map(queryableDruidServer -> queryableDruidServer.getServer().toImmutableDruidServer())
            .collect(Collectors.toList());
  }
}
