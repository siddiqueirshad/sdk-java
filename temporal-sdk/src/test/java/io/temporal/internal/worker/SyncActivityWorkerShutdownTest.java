package io.temporal.internal.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.temporal.api.workflowservice.v1.GetSystemInfoResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.interceptors.WorkerInterceptor;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.tuning.FixedSizeSlotSupplier;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.Test;

public class SyncActivityWorkerShutdownTest {
  @Test
  public void awaitTerminationWaitsForHeartbeatContinuation() throws Exception {
    SyncActivityWorker worker = newWorker();
    ExecutorService waiter = Executors.newSingleThreadExecutor();
    try (PendingHeartbeatShutdown manager = new PendingHeartbeatShutdown()) {
      worker.shutdown(manager, true);
      assertTrue(manager.heartbeatExecutor.awaitTermination(10, TimeUnit.SECONDS));
      assertFalse(worker.isTerminated());

      CountDownLatch awaiting = new CountDownLatch(1);
      Future<?> result =
          waiter.submit(
              () -> {
                awaiting.countDown();
                worker.awaitTermination(10, TimeUnit.SECONDS);
              });
      assertTrue(awaiting.await(10, TimeUnit.SECONDS));
      try {
        result.get(100, TimeUnit.MILLISECONDS);
        fail("awaitTermination returned before the activity worker received shutdown");
      } catch (TimeoutException expected) {
        assertFalse(worker.isTerminated());
      }

      manager.completion.complete(null);
      result.get(10, TimeUnit.SECONDS);
      assertTrue(worker.isTerminated());
    } finally {
      waiter.shutdownNow();
      assertTrue(waiter.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 10000)
  public void zeroBudgetDoesNotWaitForHeartbeatContinuation() throws Exception {
    SyncActivityWorker worker = newWorker();
    try (PendingHeartbeatShutdown manager = new PendingHeartbeatShutdown()) {
      CompletableFuture<Void> shutdown = worker.shutdown(manager, true);
      assertTrue(manager.heartbeatExecutor.awaitTermination(10, TimeUnit.SECONDS));

      worker.awaitTermination(0, TimeUnit.SECONDS);
      assertFalse(shutdown.isDone());
      assertFalse(worker.isTerminated());

      manager.completion.complete(null);
      worker.awaitTermination(10, TimeUnit.SECONDS);
      assertTrue(worker.isTerminated());
    }
  }

  @Test(timeout = 10000)
  public void interruptionIsPreservedWhileContinuationIsPending() throws Exception {
    SyncActivityWorker worker = newWorker();
    try (PendingHeartbeatShutdown manager = new PendingHeartbeatShutdown()) {
      worker.shutdown(manager, true);
      assertTrue(manager.heartbeatExecutor.awaitTermination(10, TimeUnit.SECONDS));

      Thread.currentThread().interrupt();
      try {
        worker.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue(Thread.currentThread().isInterrupted());
        assertFalse(worker.isTerminated());
      } finally {
        Thread.interrupted();
      }

      manager.completion.complete(null);
      worker.awaitTermination(10, TimeUnit.SECONDS);
      assertTrue(worker.isTerminated());
    }
  }

  @Test(timeout = 10000)
  public void interruptingShutdownBypassesPendingGracefulSlotWait() throws Exception {
    for (boolean allowHeartbeats : new boolean[] {false, true}) {
      SyncActivityWorker worker = newWorker(allowHeartbeats);
      try (PendingHeartbeatShutdown manager = new PendingHeartbeatShutdown()) {
        CompletableFuture<Void> graceful = worker.shutdown(manager, false);
        manager.completion.complete(null);
        assertTrue(manager.supplierWaitStarted);
        assertFalse(graceful.isDone());

        CompletableFuture<Void> interrupting = worker.shutdown(manager, true);
        interrupting.get(10, TimeUnit.SECONDS);
        worker.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue(worker.isTerminated());
        assertFalse(graceful.isDone());
      }
    }
  }

  private static SyncActivityWorker newWorker() {
    return newWorker(false);
  }

  private static SyncActivityWorker newWorker(boolean allowHeartbeats) {
    WorkflowClient client = mock(WorkflowClient.class);
    WorkflowServiceStubs service = mock(WorkflowServiceStubs.class);
    when(client.getWorkflowServiceStubs()).thenReturn(service);
    when(service.getServerCapabilities())
        .thenReturn(GetSystemInfoResponse.Capabilities::getDefaultInstance);
    return new SyncActivityWorker(
        client,
        "ns",
        "tq",
        0,
        SingleWorkerOptions.newBuilder()
            .setIdentity("test-worker")
            .setDataConverter(DefaultDataConverter.STANDARD_INSTANCE)
            .setWorkerInterceptors(new WorkerInterceptor[0])
            .setAllowActivityHeartbeatDuringShutdown(allowHeartbeats)
            .setMaxHeartbeatThrottleInterval(Duration.ofSeconds(60))
            .setDefaultHeartbeatThrottleInterval(Duration.ofSeconds(30))
            .build(),
        new FixedSizeSlotSupplier<>(1),
        new NamespaceCapabilities());
  }

  private static class PendingHeartbeatShutdown extends ShutdownManager {
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final CompletableFuture<Void> supplierCompletion = new CompletableFuture<>();
    private boolean supplierWaitStarted;
    private ExecutorService heartbeatExecutor;

    @Override
    public CompletableFuture<Void> waitForSupplierPermitsReleasedUnlimited(
        TrackingSlotSupplier<?> supplier, String name) {
      supplierWaitStarted = true;
      return supplierCompletion;
    }

    @Override
    public CompletableFuture<Void> shutdownExecutor(
        ExecutorService executor, String name, Duration timeout) {
      heartbeatExecutor = executor;
      executor.shutdown();
      return completion;
    }

    @Override
    public void close() {
      completion.complete(null);
      supplierCompletion.complete(null);
      super.close();
    }
  }
}
