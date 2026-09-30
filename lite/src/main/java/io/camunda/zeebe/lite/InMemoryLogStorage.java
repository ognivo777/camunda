/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.camunda.zeebe.logstreams.storage.LogStorage;
import io.camunda.zeebe.logstreams.storage.LogStorageReader;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * An in-memory {@link LogStorage} for the lite deployment. It keeps every appended block in heap
 * memory. It is intentionally simple (no compaction, no truncation) and is meant for local,
 * single-node use where the log is only needed for the lifetime of the process.
 *
 * <p>The implementation is modeled after the {@code ListLogStorage} test utility of the
 * logstreams module, which is proven to work with the production {@code LogStream}
 * implementation.
 */
public final class InMemoryLogStorage implements LogStorage {

  private final ConcurrentNavigableMap<Long, Integer> positionIndexMapping;
  private final ConcurrentSkipListMap<Integer, Entry> entries;
  private final Set<CommitListener> commitListeners = ConcurrentHashMap.newKeySet();
  private final AtomicInteger currentIndex = new AtomicInteger(0);

  public InMemoryLogStorage() {
    entries = new ConcurrentSkipListMap<>();
    positionIndexMapping = new ConcurrentSkipListMap<>();
  }

  @Override
  public LogStorageReader newReader() {
    return new InMemoryLogStorageReader();
  }

  @Override
  public void append(
      final long lowestPosition,
      final long highestPosition,
      final BufferWriter bufferWriter,
      final AppendListener listener) {
    final var buffer = ByteBuffer.allocate(bufferWriter.getLength());
    bufferWriter.write(new UnsafeBuffer(buffer), 0);
    append(lowestPosition, highestPosition, buffer, listener);
  }

  @Override
  public void append(
      final long lowestPosition,
      final long highestPosition,
      final ByteBuffer blockBuffer,
      final AppendListener listener) {
    try {
      final var entry = new Entry(blockBuffer);
      final var index = currentIndex.getAndIncrement();
      entries.put(index, entry);
      positionIndexMapping.put(lowestPosition, index);
      listener.onWrite(index);
      listener.onCommit(index);
      commitListeners.forEach(CommitListener::onCommit);
    } catch (final Exception e) {
      listener.onWriteError(e);
    }
  }

  @Override
  public void addCommitListener(final CommitListener listener) {
    commitListeners.add(listener);
  }

  @Override
  public void removeCommitListener(final CommitListener listener) {
    commitListeners.remove(listener);
  }

  private record Entry(ByteBuffer data) {}

  private final class InMemoryLogStorageReader implements LogStorageReader {
    volatile int currentIndex;

    @Override
    public void seek(final long position) {
      currentIndex =
          Optional.ofNullable(positionIndexMapping.lowerEntry(position))
              .map(Map.Entry::getValue)
              .orElse(0);
    }

    @Override
    public void close() {
      // no resources to release
    }

    @Override
    public boolean hasNext() {
      return currentIndex >= 0 && !entries.tailMap(currentIndex).isEmpty();
    }

    @Override
    public DirectBuffer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final int index = currentIndex;
      currentIndex++;

      return new UnsafeBuffer(entries.get(index).data());
    }
  }
}
