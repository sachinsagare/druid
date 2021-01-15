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

package org.apache.druid.java.util.common.guava;


import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import com.twitter.util.JavaTimer;
import com.twitter.util.Timer;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


public class SpeculativeExecutedSequence<T> implements Sequence<T>
{
  private static final Timer LOCAL_TIMER = new JavaTimer();
  private static final FuturePool FUTURE_POOL = new ExecutorServiceFuturePool(Executors.newFixedThreadPool(5));

  private final Future<Sequence<T>> finalSequenceFuture;
  private Sequence<T> finalSequence;
  // Stats
  private static final AtomicLong BACKUP_FIRED_COUNT = new AtomicLong();
  private static final AtomicLong PRIMARY_WON_COUNT = new AtomicLong();
  private static final AtomicLong PRIMARY_LOST_COUNT = new AtomicLong();

  public static long getBackupFiredCount()
  {
    return BACKUP_FIRED_COUNT.get();
  }

  public static long getPrimaryWonCount()
  {
    return PRIMARY_WON_COUNT.get();
  }

  public static long getPrimaryLostCount()
  {
    return PRIMARY_LOST_COUNT.get();
  }

  public SpeculativeExecutedSequence(
      Supplier<Sequence<T>> primarySequenceSupplier,
      Supplier<Sequence<T>> backupSequencesSupplier,
      int speculativeExecutionWaitTimeMs
  )
  {
    Future<Sequence<T>> primaryFuture =
        FUTURE_POOL.apply(new Function0<Sequence<T>>() {
          @Override
          public Sequence<T> apply()
          {
            Sequence<T> s = primarySequenceSupplier.get();
            // Caution: only through materilizing are we sure a sequence is fully usable. This trades materialization
            // for improved availability.
            return Sequences.simple(s.toList());
          }
        });

    Supplier<Future<Sequence<T>>> backupFutureSupplier = () ->
        FUTURE_POOL.apply(new Function0<Sequence<T>>() {
          @Override
          public Sequence<T> apply()
          {
            Sequence<T> s = backupSequencesSupplier.get();
            // Caution: only through materilizing are we sure a sequence is fully usable. This trades materialization
            // for improved availability.
            return Sequences.simple(s.toList());
          }
        });

    finalSequenceFuture = primaryFuture.within(Duration.fromMilliseconds(speculativeExecutionWaitTimeMs),
                                               LOCAL_TIMER)
                                       .rescue(new Function<Throwable, Future<Sequence<T>>>() {
                                         @Override
                                         public Future<Sequence<T>> apply(Throwable t)
                                         {
                                           final Future<Sequence<T>> backupFuture = backupFutureSupplier.get();
                                           BACKUP_FIRED_COUNT.incrementAndGet();

                                           // Build information into each future as to whether this is the original
                                           // future or the backup future.
                                           final Future<Pair<Boolean, Sequence<T>>> primaryFutureWithInfo =
                                               primaryFuture.map(
                                                 new Function<Sequence<T>, Pair<Boolean, Sequence<T>>>()
                                                 {
                                                   @Override
                                                   public Pair<Boolean, Sequence<T>> apply(Sequence<T> t)
                                                   {
                                                     return Pair.of(true, t);
                                                   }
                                                 });
                                           final Future<Pair<Boolean, Sequence<T>>> backupFutureWithInfo =
                                               backupFuture.map(
                                                 new Function<Sequence<T>, Pair<Boolean, Sequence<T>>>()
                                                 {
                                                   @Override
                                                   public Pair<Boolean, Sequence<T>> apply(Sequence<T> t)
                                                   {
                                                     return Pair.of(false, t);
                                                   }
                                                 });
                                           // If there is an exception, the first future throwing the exception will
                                           // return. Instead we want the 1st future which is successful.
                                           Future<Pair<Boolean, Sequence<T>>> origFutureSuccessful =
                                               primaryFutureWithInfo.rescue(new Function<Throwable,
                                                   Future<Pair<Boolean, Sequence<T>>>>() {
                                                 @Override
                                                 public Future<Pair<Boolean, Sequence<T>>> apply(Throwable t)
                                                 {
                                                   // Fall back to back up future which may also fail in which case we
                                                   // bubble up the exception.
                                                   return backupFutureWithInfo;
                                                 }
                                               });
                                           Future<Pair<Boolean, Sequence<T>>> backupFutureSuccessful =
                                               backupFutureWithInfo.rescue(
                                                   new Function<Throwable, Future<Pair<Boolean, Sequence<T>>>>()
                                                   {
                                                     @Override
                                                     public Future<Pair<Boolean, Sequence<T>>> apply(Throwable t)
                                                     {
                                                       // Fall back to original Future which may also fail in which case
                                                       // we bubble up the exception.
                                                       return primaryFutureWithInfo;
                                                     }
                                                 });

                                           return origFutureSuccessful.select(backupFutureSuccessful).map(
                                               new Function<Pair<Boolean, Sequence<T>>, Sequence<T>>()
                                               {
                                                 @Override
                                                 public Sequence<T> apply(Pair<Boolean, Sequence<T>> pair)
                                                 {
                                                   if (pair.lhs) {
                                                     PRIMARY_WON_COUNT.incrementAndGet();
                                                   } else {
                                                     PRIMARY_LOST_COUNT.incrementAndGet();
                                                   }
                                                   return pair.rhs;
                                                 }
                                               });
                                         }
                                       });
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    finalizeSequence();
    return this.finalSequence.accumulate(initValue, accumulator);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    finalizeSequence();
    return this.finalSequence.toYielder(initValue, accumulator);
  }

  private synchronized void finalizeSequence()
  {
    if (this.finalSequence == null) {
      try {
        this.finalSequence = Await.result(finalSequenceFuture);
      }
      catch (Exception e) {
        throw new RE(
            e,
            "Failure getting results from sequence because of [%s]",
            e.getMessage()
        );
      }
    }
  }
}
