package io.astradb.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class HybridClockTest {

  @Test
  void constructorShouldInitializeClock() {
    HybridClock clock = new HybridClock();

    HybridTimestamp timestamp = clock.peek();

    assertNotNull(timestamp);
    assertTrue(timestamp.getPhysicalTime() > 0);
    assertEquals(0, timestamp.getLogicalCounter());
  }

  @Test
  void nowShouldGenerateMonotonicallyIncreasingTimestamps() {
    HybridClock clock = new HybridClock();

    HybridTimestamp first = clock.now();
    HybridTimestamp second = clock.now();
    HybridTimestamp third = clock.now();

    assertTrue(first.compareTo(second) < 0);
    assertTrue(second.compareTo(third) < 0);
  }

  @Test
  void nowShouldHandleMultipleCallsWithinSameMillisecond() {
    HybridClock clock = new HybridClock();

    HybridTimestamp first = clock.now();
    HybridTimestamp second = clock.now();

    if (first.getPhysicalTime() == second.getPhysicalTime()) {
      assertEquals(first.getLogicalCounter() + 1, second.getLogicalCounter());
    } else {
      assertTrue(second.getPhysicalTime() > first.getPhysicalTime());
      assertEquals(0, second.getLogicalCounter());
    }
  }

  @Test
  void peekShouldNotModifyClockState() {
    HybridClock clock = new HybridClock();

    HybridTimestamp firstPeek = clock.peek();
    HybridTimestamp secondPeek = clock.peek();

    assertEquals(firstPeek, secondPeek);
  }

  @Test
  void peekShouldReturnLatestGeneratedTimestamp() {
    HybridClock clock = new HybridClock();

    HybridTimestamp generated = clock.now();

    assertEquals(generated, clock.peek());
  }

  @Test
  void updateShouldAdvanceClockWhenRemoteTimestampIsAhead() {
    HybridClock clock = new HybridClock();

    HybridTimestamp remote = new HybridTimestamp(System.currentTimeMillis() + 10_000, 5);

    clock.update(remote);

    HybridTimestamp updated = clock.peek();

    assertEquals(remote.getPhysicalTime(), updated.getPhysicalTime());
    assertEquals(remote.getLogicalCounter() + 1, updated.getLogicalCounter());
  }

  @Test
  void updateShouldPreserveLocalClockWhenRemoteTimestampIsBehind() {
    HybridClock clock = new HybridClock();

    HybridTimestamp local = new HybridTimestamp(System.currentTimeMillis() + 20_000, 10);

    clock.update(local);

    HybridTimestamp remote = new HybridTimestamp(local.getPhysicalTime() - 5_000, 3);

    clock.update(remote);

    HybridTimestamp updated = clock.peek();

    assertEquals(local.getPhysicalTime(), updated.getPhysicalTime());
    assertEquals(local.getLogicalCounter() + 2, updated.getLogicalCounter());
  }

  @Test
  void updateShouldIncrementMaximumLogicalCounterWhenPhysicalTimesMatch() {
    HybridClock clock = new HybridClock();

    HybridTimestamp local = new HybridTimestamp(System.currentTimeMillis() + 10_000, 4);

    clock.update(local);

    HybridTimestamp remote = new HybridTimestamp(local.getPhysicalTime(), 8);

    clock.update(remote);

    HybridTimestamp updated = clock.peek();

    assertEquals(local.getPhysicalTime(), updated.getPhysicalTime());
    assertEquals(9, updated.getLogicalCounter());
  }

  @Test
  void nowShouldGenerateTimestampAfterRemoteUpdate() {
    HybridClock clock = new HybridClock();

    HybridTimestamp remote = new HybridTimestamp(System.currentTimeMillis() + 10_000, 100);

    clock.update(remote);

    HybridTimestamp next = clock.now();

    assertTrue(next.compareTo(remote) > 0);
  }

  @Test
  void updateShouldRejectNullTimestamp() {
    HybridClock clock = new HybridClock();

    assertThrows(NullPointerException.class, () -> clock.update(null));
  }

  @Test
  void concurrentNowCallsShouldGenerateUniqueMonotonicTimestamps() throws Exception {

    int threadCount = 8;
    int timestampsPerThread = 1_000;

    HybridClock clock = new HybridClock();
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<List<HybridTimestamp>>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < threadCount; i++) {
        futures.add(
            executor.submit(
                () -> {
                  startLatch.await();

                  List<HybridTimestamp> timestamps = new ArrayList<>();

                  for (int j = 0; j < timestampsPerThread; j++) {
                    timestamps.add(clock.now());
                  }

                  return timestamps;
                }));
      }

      startLatch.countDown();

      List<HybridTimestamp> allTimestamps = new ArrayList<>();

      for (Future<List<HybridTimestamp>> future : futures) {
        allTimestamps.addAll(future.get(10, TimeUnit.SECONDS));
      }

      Collections.sort(allTimestamps);

      assertEquals(threadCount * timestampsPerThread, allTimestamps.size());

      for (int i = 1; i < allTimestamps.size(); i++) {
        assertTrue(
            allTimestamps.get(i - 1).compareTo(allTimestamps.get(i)) < 0,
            "Timestamps must be strictly increasing and unique");
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void concurrentUpdateAndNowShouldMaintainMonotonicity() throws Exception {

    HybridClock clock = new HybridClock();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);

    try {
      Future<?> updateTask =
          executor.submit(
              () -> {
                startLatch.await();

                for (int i = 0; i < 1_000; i++) {
                  clock.update(new HybridTimestamp(System.currentTimeMillis() + i, i));
                }

                return null;
              });

      Future<List<HybridTimestamp>> nowTask =
          executor.submit(
              () -> {
                startLatch.await();

                List<HybridTimestamp> timestamps = new ArrayList<>();

                for (int i = 0; i < 1_000; i++) {
                  timestamps.add(clock.now());
                }

                return timestamps;
              });

      startLatch.countDown();

      updateTask.get(10, TimeUnit.SECONDS);
      List<HybridTimestamp> timestamps = nowTask.get(10, TimeUnit.SECONDS);

      for (int i = 1; i < timestamps.size(); i++) {
        assertTrue(timestamps.get(i - 1).compareTo(timestamps.get(i)) < 0);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void updateShouldSupportLogicalCountersBeyondIntegerMaxValue() {
    HybridClock clock = new HybridClock();

    long largeCounter = (long) Integer.MAX_VALUE + 100;

    HybridTimestamp remote = new HybridTimestamp(System.currentTimeMillis() + 10_000, largeCounter);

    clock.update(remote);

    HybridTimestamp updated = clock.peek();

    assertEquals(largeCounter + 1, updated.getLogicalCounter());
    assertTrue(updated.compareTo(remote) > 0);
  }

  @Test
  void shouldUseInjectedClockForTimestampGeneration() {
    ManualClock manualClock = new ManualClock(1000L);
    HybridClock clock = new HybridClock(manualClock);

    HybridTimestamp first = clock.now();

    assertEquals(1000L, first.getPhysicalTime());
    assertEquals(1L, first.getLogicalCounter());

    manualClock.advanceMillis(10L);

    HybridTimestamp second = clock.now();

    assertEquals(1010L, second.getPhysicalTime());
    assertEquals(0L, second.getLogicalCounter());
  }

  @Test
  void shouldRemainMonotonicWhenPhysicalClockMovesBackward() {
    ManualClock manualClock = new ManualClock(1000L);
    HybridClock clock = new HybridClock(manualClock);

    HybridTimestamp first = clock.now();

    manualClock.setTimeMillis(500L);

    HybridTimestamp second = clock.now();

    assertTrue(second.compareTo(first) > 0);
    assertEquals(first.getPhysicalTime(), second.getPhysicalTime());
  }

  @Test
  void shouldSynchronizeWithRemoteTimestampUsingInjectedClock() {
    ManualClock manualClock = new ManualClock(1000L);
    HybridClock clock = new HybridClock(manualClock);

    HybridTimestamp remote = new HybridTimestamp(5000L, 20L);

    clock.update(remote);

    HybridTimestamp updated = clock.peek();

    assertEquals(5000L, updated.getPhysicalTime());
    assertEquals(21L, updated.getLogicalCounter());
  }

  @Test
  void constructorShouldRejectNullClock() {
    assertThrows(NullPointerException.class, () -> new HybridClock(null));
  }

  @Test
  void shouldAdvancePhysicalTimeWhenLogicalCounterOverflowsInNow() {
    ManualClock clock = new ManualClock(1);
    HybridClock hybridClock = new HybridClock(clock);

    hybridClock.update(new HybridTimestamp(10, Long.MAX_VALUE - 1));

    HybridTimestamp timestamp = hybridClock.now();

    assertEquals(11, timestamp.getPhysicalTime());
    assertEquals(0, timestamp.getLogicalCounter());
  }

  @Test
  void shouldAdvancePhysicalTimeWhenLogicalCounterOverflowsInUpdate() {
    ManualClock clock = new ManualClock(1);
    HybridClock hybridClock = new HybridClock(clock);

    HybridTimestamp timestamp = hybridClock.update(new HybridTimestamp(10, Long.MAX_VALUE));

    assertEquals(11, timestamp.getPhysicalTime());
    assertEquals(0, timestamp.getLogicalCounter());
  }

  @Test
  void shouldThrowWhenPhysicalTimeAndLogicalCounterCannotAdvance() {
    ManualClock clock = new ManualClock(1);
    HybridClock hybridClock = new HybridClock(clock);

    hybridClock.update(new HybridTimestamp(Long.MAX_VALUE, Long.MAX_VALUE - 1));

    assertThrows(ArithmeticException.class, hybridClock::now);
  }

  @Test
  void shouldThrowWhenUpdateCannotAdvanceBeyondMaximumTimestamp() {
    ManualClock clock = new ManualClock(1);
    HybridClock hybridClock = new HybridClock(clock);

    assertThrows(
        ArithmeticException.class,
        () -> hybridClock.update(new HybridTimestamp(Long.MAX_VALUE, Long.MAX_VALUE)));
  }

  @Test
  void shouldRejectClockReturningNegativeInitialTime() {
    Clock clock = () -> -1L;

    assertThrows(IllegalArgumentException.class, () -> new HybridClock(clock));
  }
}
