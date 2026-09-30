/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import io.atomix.cluster.MemberId;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerClusterState;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerClusterStateImpl;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerTopologyListener;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.camunda.zeebe.protocol.record.PartitionHealthStatus;
import io.camunda.zeebe.topology.state.ClusterTopology;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link BrokerTopologyManager} with a fixed, static topology: one broker (this node) leading a
 * fixed number of partitions. It replaces the atomix-backed {@code BrokerTopologyManagerImpl},
 * which is not needed for a single-node deployment.
 */
public final class StaticBrokerTopologyManager implements BrokerTopologyManager {

  private final int nodeId;
  private final BrokerClusterState topology;
  private final Set<BrokerTopologyListener> listeners = ConcurrentHashMap.newKeySet();

  public StaticBrokerTopologyManager(final int nodeId, final int partitionsCount) {
    this.nodeId = nodeId;

    final var state = new BrokerClusterStateImpl();
    state.addBrokerIfAbsent(nodeId);
    state.setBrokerAddressIfPresent(nodeId, "127.0.0.1:26500");
    state.setBrokerVersionIfPresent(nodeId, "8.4.12-lite");
    state.setClusterSize(1);
    state.setPartitionsCount(partitionsCount);
    state.setReplicationFactor(1);
    for (int partitionId = 1; partitionId <= partitionsCount; partitionId++) {
      state.addPartitionIfAbsent(partitionId);
      state.setPartitionLeader(partitionId, nodeId, 1L);
      state.setPartitionHealthStatus(nodeId, partitionId, PartitionHealthStatus.HEALTHY);
    }
    this.topology = state;
  }

  @Override
  public BrokerClusterState getTopology() {
    return topology;
  }

  @Override
  public void addTopologyListener(final BrokerTopologyListener listener) {
    if (listeners.add(listener)) {
      listener.brokerAdded(new MemberId(String.valueOf(nodeId)));
    }
  }

  @Override
  public void removeTopologyListener(final BrokerTopologyListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void onTopologyUpdated(final ClusterTopology clusterTopology) {
    // static topology: it never changes
  }
}
