/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.broker.system.configuration.engine;

import io.camunda.zeebe.broker.system.configuration.ConfigurationEntry;
import io.camunda.zeebe.engine.EngineConfiguration;
import java.time.Duration;

public class SecretResolutionCfg implements ConfigurationEntry {
  private Duration interval = EngineConfiguration.DEFAULT_SECRET_RESOLUTION_INTERVAL;
  private int retryMaxAttempts = EngineConfiguration.DEFAULT_SECRET_RESOLUTION_RETRY_MAX_ATTEMPTS;
  private Duration retryInitialDelay =
      EngineConfiguration.DEFAULT_SECRET_RESOLUTION_RETRY_INITIAL_DELAY;
  private Duration retryMaxDelay = EngineConfiguration.DEFAULT_SECRET_RESOLUTION_RETRY_MAX_DELAY;
  private int retryBackoffFactor =
      EngineConfiguration.DEFAULT_SECRET_RESOLUTION_RETRY_BACKOFF_FACTOR;

  public Duration getInterval() {
    return interval;
  }

  public void setInterval(final Duration interval) {
    this.interval = interval;
  }

  public int getRetryMaxAttempts() {
    return retryMaxAttempts;
  }

  public void setRetryMaxAttempts(final int retryMaxAttempts) {
    this.retryMaxAttempts = retryMaxAttempts;
  }

  public Duration getRetryInitialDelay() {
    return retryInitialDelay;
  }

  public void setRetryInitialDelay(final Duration retryInitialDelay) {
    this.retryInitialDelay = retryInitialDelay;
  }

  public Duration getRetryMaxDelay() {
    return retryMaxDelay;
  }

  public void setRetryMaxDelay(final Duration retryMaxDelay) {
    this.retryMaxDelay = retryMaxDelay;
  }

  public int getRetryBackoffFactor() {
    return retryBackoffFactor;
  }

  public void setRetryBackoffFactor(final int retryBackoffFactor) {
    this.retryBackoffFactor = retryBackoffFactor;
  }

  @Override
  public String toString() {
    return "SecretResolutionCfg{"
        + "interval="
        + interval
        + ", retryMaxAttempts="
        + retryMaxAttempts
        + ", retryInitialDelay="
        + retryInitialDelay
        + ", retryMaxDelay="
        + retryMaxDelay
        + ", retryBackoffFactor="
        + retryBackoffFactor
        + '}';
  }
}
