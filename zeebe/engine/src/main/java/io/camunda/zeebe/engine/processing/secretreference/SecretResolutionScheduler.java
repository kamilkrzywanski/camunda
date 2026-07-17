/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.secretreference;

import io.camunda.secretstore.SecretCache;
import io.camunda.secretstore.SecretResolutionResult;
import io.camunda.secretstore.SecretStore;
import io.camunda.secretstore.SecretStoreUnavailableException;
import io.camunda.zeebe.engine.EngineConfiguration;
import io.camunda.zeebe.engine.state.immutable.ScheduledTaskState;
import io.camunda.zeebe.engine.state.immutable.SecretReferenceState;
import io.camunda.zeebe.protocol.impl.record.value.secretreference.SecretReferenceRecord;
import io.camunda.zeebe.protocol.record.intent.SecretReferenceIntent;
import io.camunda.zeebe.stream.api.ReadonlyStreamProcessorContext;
import io.camunda.zeebe.stream.api.StreamProcessorLifecycleAware;
import io.camunda.zeebe.stream.api.scheduling.AsyncTaskGroup;
import io.camunda.zeebe.stream.api.scheduling.TaskResult;
import io.camunda.zeebe.stream.api.scheduling.TaskResultBuilder;
import java.time.Duration;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reads all pending secret references from state, resolves them in batches (one batch
 * per store), populates the cache with resolved values, and writes {@link
 * SecretReferenceIntent#RESOLUTION_COMPLETE} or {@link SecretReferenceIntent#RESOLUTION_FAIL}
 * commands.
 *
 * <p>Two distinct failure modes:
 *
 * <ul>
 *   <li>{@link SecretResolutionResult.Failed} — permanent per-secret error (NOT_FOUND,
 *       ACCESS_DENIED, INVALID_REF): write {@code RESOLUTION_FAIL} immediately, no retry, no cache
 *       write.
 *   <li>{@link SecretStoreUnavailableException} — transient store-level failure: retry the whole
 *       store with exponential backoff. After {@code retryMaxAttempts} failures, write {@code
 *       RESOLUTION_FAIL} for all pending refs in that store.
 * </ul>
 *
 * <p>Retry state ({@code attempts} + {@code nextAttemptAt} per storeId) is in-memory only and
 * intentionally resets on broker restart / partition failover.
 *
 * <p>Runs on {@link AsyncTaskGroup#SECRET_RESOLUTION} (IO-bound) so the stream processor is never
 * blocked by store IO.
 */
public final class SecretResolutionScheduler implements StreamProcessorLifecycleAware {

  private static final Logger LOG = LoggerFactory.getLogger(SecretResolutionScheduler.class);

  private final SecretReferenceState secretReferenceState;
  private final Map<String, SecretStore> secretStores;
  private final Map<String, SecretCache> secretCaches;
  private final Duration schedulingInterval;
  private final Duration retryInitialDelay;
  private final Duration retryMaxDelay;
  private final int retryMaxAttempts;
  private final int retryBackoffFactor;

  private final Map<String, StoreRetryState> storeRetryStates = new HashMap<>();

  private ReadonlyStreamProcessorContext processingContext;
  private InstantSource clock;

  public SecretResolutionScheduler(
      final Supplier<ScheduledTaskState> scheduledTaskStateFactory,
      final Map<String, SecretStore> secretStores,
      final Map<String, SecretCache> secretCaches,
      final EngineConfiguration config) {
    secretReferenceState = scheduledTaskStateFactory.get().getSecretReferenceState();
    this.secretStores = secretStores;
    this.secretCaches = secretCaches;
    schedulingInterval = config.getSecretResolutionInterval();
    retryInitialDelay = config.getSecretResolutionRetryInitialDelay();
    retryMaxDelay = config.getSecretResolutionRetryMaxDelay();
    retryMaxAttempts = config.getSecretResolutionRetryMaxAttempts();
    retryBackoffFactor = config.getSecretResolutionRetryBackoffFactor();
  }

  @Override
  public void onRecovered(final ReadonlyStreamProcessorContext context) {
    processingContext = context;
    clock = context.getClock();
    scheduleNext(schedulingInterval);
  }

  @Override
  public void onResumed() {
    scheduleNext(schedulingInterval);
  }

  TaskResult resolveSecrets(final TaskResultBuilder resultBuilder) {
    final long now = clock.millis();
    try {
      final Map<String, Set<String>> pendingByStore = collectPendingByStore();
      // prune retry state for stores that no longer have pending refs
      storeRetryStates.keySet().retainAll(pendingByStore.keySet());
      if (!pendingByStore.isEmpty()) {
        for (final var entry : pendingByStore.entrySet()) {
          resolveStore(entry.getKey(), entry.getValue(), resultBuilder, now);
        }
      }
    } finally {
      scheduleNext(computeNextDelay(now));
    }
    return resultBuilder.build();
  }

  /**
   * Returns the delay until the next execution. If any store is in cooldown with a retry deadline
   * sooner than {@code schedulingInterval}, returns the shorter duration so backoff is honored.
   */
  private Duration computeNextDelay(final long now) {
    if (storeRetryStates.isEmpty()) {
      return schedulingInterval;
    }
    final long earliestRetryAt =
        storeRetryStates.values().stream()
            .mapToLong(StoreRetryState::nextAttemptAt)
            .min()
            .getAsLong();
    final long millisUntilRetry = earliestRetryAt - now;
    if (millisUntilRetry < schedulingInterval.toMillis()) {
      return Duration.ofMillis(Math.max(0, millisUntilRetry));
    }
    return schedulingInterval;
  }

  private Map<String, Set<String>> collectPendingByStore() {
    final Map<String, Set<String>> pendingByStore = new LinkedHashMap<>();
    secretReferenceState.visitPendingSecretReferences(
        (storeId, secretRef) ->
            pendingByStore.computeIfAbsent(storeId, k -> new LinkedHashSet<>()).add(secretRef));
    return pendingByStore;
  }

  private void resolveStore(
      final String storeId,
      final Set<String> refs,
      final TaskResultBuilder resultBuilder,
      final long now) {
    final StoreRetryState retryState =
        storeRetryStates.getOrDefault(storeId, StoreRetryState.INITIAL);

    if (now < retryState.nextAttemptAt()) {
      LOG.trace(
          "Secret store '{}' in cooldown until {}ms, skipping {} pending refs",
          storeId,
          retryState.nextAttemptAt(),
          refs.size());
      return;
    }

    final SecretStore store = secretStores.get(storeId);
    if (store == null) {
      LOG.warn(
          "Secret store '{}' is not configured — failing {} pending secret refs",
          storeId,
          refs.size());
      refs.forEach(ref -> appendResolutionFail(resultBuilder, storeId, ref));
      storeRetryStates.remove(storeId);
      return;
    }

    try {
      final Map<String, SecretResolutionResult> results = store.resolve(refs);
      final SecretCache cache = secretCaches.get(storeId);
      results.forEach(
          (ref, result) -> {
            switch (result) {
              case SecretResolutionResult.Resolved(final String value) -> {
                if (cache != null) {
                  cache.put(ref, value);
                }
                appendResolutionComplete(resultBuilder, storeId, ref);
              }
              case SecretResolutionResult.Failed(
                      final var code,
                      final var message,
                      final var cause) -> {
                LOG.warn(
                    "Secret '{}' in secret store '{}' failed permanently: {} — {}",
                    ref,
                    storeId,
                    code,
                    message,
                    cause);
                appendResolutionFail(resultBuilder, storeId, ref);
              }
            }
          });
      storeRetryStates.remove(storeId);
    } catch (final SecretStoreUnavailableException e) {
      final int nextAttempts = retryState.attempts() + 1;
      if (nextAttempts >= retryMaxAttempts) {
        LOG.warn(
            "Secret store '{}' unavailable after {}/{} attempts — failing {} pending refs: {}",
            storeId,
            nextAttempts,
            retryMaxAttempts,
            refs.size(),
            e.getMessage());
        refs.forEach(ref -> appendResolutionFail(resultBuilder, storeId, ref));
        storeRetryStates.remove(storeId);
      } else {
        final Duration backoff = calculateBackoff(nextAttempts);
        LOG.warn(
            "Secret store '{}' unavailable (attempt {}/{}), retrying in {}: {}",
            storeId,
            nextAttempts,
            retryMaxAttempts,
            backoff,
            e.getMessage());
        storeRetryStates.put(storeId, new StoreRetryState(nextAttempts, now + backoff.toMillis()));
      }
    }
  }

  private void appendResolutionComplete(
      final TaskResultBuilder resultBuilder, final String storeId, final String secretRef) {
    final var record = new SecretReferenceRecord();
    record.setStoreId(storeId).setSecretReference(secretRef);
    resultBuilder.appendCommandRecord(SecretReferenceIntent.RESOLUTION_COMPLETE, record);
  }

  private void appendResolutionFail(
      final TaskResultBuilder resultBuilder, final String storeId, final String secretRef) {
    final var record = new SecretReferenceRecord();
    record.setStoreId(storeId).setSecretReference(secretRef);
    resultBuilder.appendCommandRecord(SecretReferenceIntent.RESOLUTION_FAIL, record);
  }

  private Duration calculateBackoff(final int attempts) {
    Duration delay = retryInitialDelay;
    for (int i = 1; i < attempts; i++) {
      delay = delay.multipliedBy(retryBackoffFactor);
      if (delay.compareTo(retryMaxDelay) >= 0) {
        return retryMaxDelay;
      }
    }
    return delay;
  }

  private void scheduleNext(final Duration delay) {
    processingContext
        .getScheduleService()
        .runDelayedAsync(delay, this::resolveSecrets, AsyncTaskGroup.SECRET_RESOLUTION);
  }

  /** In-memory retry state for a single store. Resets on broker restart (by design). */
  record StoreRetryState(int attempts, long nextAttemptAt) {
    static final StoreRetryState INITIAL = new StoreRetryState(0, 0L);
  }
}
