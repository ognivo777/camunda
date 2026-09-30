/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.scheduling.ScheduledCommandCache.StageableScheduledCommandCache;
import io.camunda.zeebe.stream.api.scheduling.ScheduledCommandCache.StagedScheduledCommandCache;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory {@link StageableScheduledCommandCache} used by the stream processor to dedupe the
 * commands scheduled by the engine's periodic checkers (timer triggers, job timeouts, job
 * backoffs, message expiries).
 *
 * <p>Same semantics as the broker's {@code BoundedScheduledCommandCache} — it only tracks
 * commands for the intents it was created with, and other intents are ignored — but without the
 * per-intent bound, which is irrelevant for a single, unbounded local partition.
 *
 * <p>NOTE: like in the stock implementation, the staged cache returned via {@link #stage()} is
 * not thread-safe; only the main cache is.
 */
public final class InMemoryScheduledCommandCache implements StageableScheduledCommandCache {

  private final Map<Intent, Set<Long>> caches;

  /**
   * Creates a cache which will only track commands for the given intents.
   *
   * @param intents intents whose scheduled commands should be deduped
   */
  public InMemoryScheduledCommandCache(final Intent... intents) {
    final Map<Intent, Set<Long>> cacheMap = new HashMap<>();
    for (final var intent : intents) {
      cacheMap.put(intent, ConcurrentHashMap.newKeySet());
    }
    this.caches = cacheMap;
  }

  @Override
  public void add(final Intent intent, final long key) {
    final var cache = caches.get(intent);
    if (cache != null) {
      cache.add(key);
    }
  }

  @Override
  public boolean contains(final Intent intent, final long key) {
    final var cache = caches.get(intent);
    return cache != null && cache.contains(key);
  }

  @Override
  public void remove(final Intent intent, final long key) {
    final var cache = caches.get(intent);
    if (cache != null) {
      cache.remove(key);
    }
  }

  @Override
  public void clear() {
    caches.values().forEach(Set::clear);
  }

  @Override
  public StagedScheduledCommandCache stage() {
    return new StagedCache();
  }

  private final class StagedCache implements StagedScheduledCommandCache {
    private final Map<Intent, Set<Long>> stagedKeys = new HashMap<>();

    @Override
    public void add(final Intent intent, final long key) {
      stagedKeys(intent).add(key);
    }

    @Override
    public boolean contains(final Intent intent, final long key) {
      return stagedKeys(intent).contains(key)
          || InMemoryScheduledCommandCache.this.contains(intent, key);
    }

    @Override
    public void remove(final Intent intent, final long key) {
      stagedKeys(intent).remove(key);
    }

    @Override
    public void clear() {
      stagedKeys.values().forEach(Set::clear);
    }

    @Override
    public void persist() {
      for (final var entry : stagedKeys.entrySet()) {
        final var cache = caches.get(entry.getKey());
        if (cache != null) {
          cache.addAll(entry.getValue());
        }
      }
    }

    private Set<Long> stagedKeys(final Intent intent) {
      return stagedKeys.computeIfAbsent(intent, ignored -> ConcurrentHashMap.newKeySet());
    }
  }
}
