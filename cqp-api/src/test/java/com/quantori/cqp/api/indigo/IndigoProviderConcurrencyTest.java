package com.quantori.cqp.api.indigo;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.epam.indigo.Indigo;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class IndigoProviderConcurrencyTest {

  private final int maxPoolSize = 100;
  private final IndigoProvider indigoProvider = new IndigoProvider(maxPoolSize, 5);

  @Test
  void testIndigoProviderConcurrency_when4000Threads_thenOK() throws InterruptedException {
    int totalThreads = 4000;

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(totalThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);

    // Spawn threads to acquire and release Indigo objects
    for (int i = 0; i < totalThreads; i++) {
      executorService.submit(() -> {
        try {
          startLatch.await(); // Wait until the start signal is given
          Indigo indigo = indigoProvider.take();
          try {
            simulateProcessing();
          } finally {
            indigoProvider.offer(indigo);
          }
        } catch (Exception e) {
          Thread.currentThread().interrupt();
        } finally {
          finishLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    finishLatch.await();
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  void testIndigoProviderConcurrency_when1000ThreadsAndDuplicates_thenOK() throws InterruptedException {
    int totalThreads = 1000;

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(totalThreads);
    ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);

    // Spawn threads to acquire and release Indigo objects
    for (int i = 0; i < totalThreads; i++) {
      executorService.submit(() -> {
        try {
          startLatch.await(); // Wait until the start signal is given
          Indigo indigo = indigoProvider.take();
          boolean offerDuplicate = false;
          try {
            simulateProcessing();
            if (Math.random() > 0.2) {
              offerDuplicate = true;
              indigoProvider.offer(indigo);
            }
          } finally {
            if (offerDuplicate) {
              assertThrows(ObjectPoolException.class, () -> indigoProvider.offer(indigo));
            } else {
              indigoProvider.offer(indigo);
            }
          }
        } catch (Exception e) {
          Thread.currentThread().interrupt();
        } finally {
          finishLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    finishLatch.await();
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  void testIndigoProviderConcurrency_whenBadIndigoProvider_thenAssertThrowsOK() {
    IndigoProvider badIndigoProvider = new IndigoProvider(1, 1);
    badIndigoProvider.take();
    assertThrows(ObjectPoolException.class, badIndigoProvider::take);
  }

  private void simulateProcessing() throws InterruptedException {

    // Simulate work by sleeping for a random duration between 200ms and 0.5s
    if (Math.random() > 0.3) {

      int minWaitTime = 200;
      int maxWaitTime = 500;
      Random random = new Random();
      int waitTime = random.nextInt(maxWaitTime - minWaitTime + 1) + minWaitTime;
      Thread.sleep(waitTime);

    } else {

      throw new RuntimeException();
    }
  }
}
