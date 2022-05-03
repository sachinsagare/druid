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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;


public class SpeculativeExecutedSequenceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final int SPECULATIVE_EXECUTION_WAIT_TIME_MS = 3000;
  private static final List<Integer> EXPECTED_PRIMARY = ImmutableList.of(1, 2, 3);
  private static final List<Integer> EXPECTED_BACKUP = ImmutableList.of(4, 5, 6);

  @Test
  public void testBackupNotTriggeredBecausePrimarySucceedsQuickly() throws Exception
  {
    AtomicBoolean primaryTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> primarySequenceSupplier = () -> {
      primaryTriggered.set(true);
      return Sequences.simple(EXPECTED_PRIMARY);
    };

    AtomicBoolean backupTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> backupSequencesSupplier = () -> {
      backupTriggered.set(true);
      return Sequences.simple(EXPECTED_BACKUP);
    };

    SpeculativeExecutedSequence<Integer> actual = new SpeculativeExecutedSequence<>(primarySequenceSupplier,
                                                                                    backupSequencesSupplier,
                                                                                    SPECULATIVE_EXECUTION_WAIT_TIME_MS);
    // Sleep long enough to wait for possible backup to fire
    Thread.sleep(SPECULATIVE_EXECUTION_WAIT_TIME_MS * 2);

    SequenceTestHelper.testAll(actual, EXPECTED_PRIMARY);
    Assert.assertTrue(primaryTriggered.get());
    Assert.assertFalse(backupTriggered.get());
  }

  @Test
  public void testBackupWinsBecausePrimaryThrowsException() throws Exception
  {
    AtomicBoolean primaryTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> primarySequenceSupplier = () -> {
      primaryTriggered.set(true);
      throw new RuntimeException("Primary sequence runtime exception");
    };

    AtomicBoolean backupTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> backupSequencesSupplier = () -> {
      backupTriggered.set(true);
      return Sequences.simple(EXPECTED_BACKUP);
    };

    SpeculativeExecutedSequence<Integer> actual = new SpeculativeExecutedSequence<>(primarySequenceSupplier,
                                                                                    backupSequencesSupplier,
                                                                                    SPECULATIVE_EXECUTION_WAIT_TIME_MS);
    // Sleep long enough to wait for possible backup to fire
    Thread.sleep(SPECULATIVE_EXECUTION_WAIT_TIME_MS * 2);

    SequenceTestHelper.testAll(actual, EXPECTED_BACKUP);
    Assert.assertTrue(primaryTriggered.get());
    Assert.assertTrue(backupTriggered.get());
  }

  @Test
  public void testBackupWinsBecausePrimaryHangs() throws Exception
  {
    AtomicBoolean primaryTriggered = new AtomicBoolean();
    AtomicBoolean primaryReturned = new AtomicBoolean();
    Supplier<Sequence<Integer>> primarySequenceSupplier = () -> {
      primaryTriggered.set(true);
      try {
        // Hang for a long time to simulate hanging
        Thread.sleep(30000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
      primaryReturned.set(true);
      return Sequences.simple(EXPECTED_PRIMARY);
    };

    AtomicBoolean backupTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> backupSequencesSupplier = () -> {
      backupTriggered.set(true);
      return Sequences.simple(EXPECTED_BACKUP);
    };

    SpeculativeExecutedSequence<Integer> actual = new SpeculativeExecutedSequence<>(primarySequenceSupplier,
                                                                                    backupSequencesSupplier,
                                                                                    SPECULATIVE_EXECUTION_WAIT_TIME_MS);
    // Sleep long enough to wait for possible backup to fire
    Thread.sleep(SPECULATIVE_EXECUTION_WAIT_TIME_MS * 2);

    SequenceTestHelper.testAll(actual, EXPECTED_BACKUP);
    Assert.assertTrue(primaryTriggered.get());
    Assert.assertFalse(primaryReturned.get());
    Assert.assertTrue(backupTriggered.get());
  }

  @Test
  public void testBothPrimaryBackupTriggerException() throws Exception
  {
    AtomicBoolean primaryTriggered = new AtomicBoolean();
    AtomicBoolean primaryReturned = new AtomicBoolean();
    Supplier<Sequence<Integer>> primarySequenceSupplier = () -> {
      primaryTriggered.set(true);
      throw new RuntimeException("Primary sequence runtime exception");
    };

    AtomicBoolean backupTriggered = new AtomicBoolean();
    Supplier<Sequence<Integer>> backupSequencesSupplier = () -> {
      backupTriggered.set(true);
      throw new RuntimeException("Backup sequence runtime exception");
    };

    SpeculativeExecutedSequence<Integer> actual = new SpeculativeExecutedSequence<>(primarySequenceSupplier,
                                                                                    backupSequencesSupplier,
                                                                                    SPECULATIVE_EXECUTION_WAIT_TIME_MS);
    // Sleep long enough to wait for possible backup to fire
    Thread.sleep(SPECULATIVE_EXECUTION_WAIT_TIME_MS * 2);

    expectedException.expect(RuntimeException.class);
    // Can be either primary or backup
    expectedException.expectMessage("sequence runtime exception");
    SequenceTestHelper.testAll(actual, EXPECTED_BACKUP);
    Assert.assertTrue(primaryTriggered.get());
    Assert.assertTrue(backupTriggered.get());
  }
}
