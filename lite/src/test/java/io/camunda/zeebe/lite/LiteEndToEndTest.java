/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.lite;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ProcessInstanceResult;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end tests against the full lite runtime using the official {@link ZeebeClient}: deploy
 * BPMN processes, create instances, let job workers complete jobs and assert the instances
 * finish with the expected variables. Also verifies that timer events (scheduled by the
 * engine's due-date checker over the scheduled-command cache) fire.
 */
class LiteEndToEndTest {

  private static final String PROCESS_XML =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
          targetNamespace="http://camunda.org/lite" id="lite-test-definitions">
        <bpmn:process id="lite-test-process" isExecutable="true">
          <bpmn:startEvent id="start">
            <bpmn:outgoing>flow-start</bpmn:outgoing>
          </bpmn:startEvent>
          <bpmn:serviceTask id="task">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="lite-test-job"/>
            </bpmn:extensionElements>
            <bpmn:incoming>flow-start</bpmn:incoming>
            <bpmn:outgoing>flow-end</bpmn:outgoing>
          </bpmn:serviceTask>
          <bpmn:endEvent id="end">
            <bpmn:incoming>flow-end</bpmn:incoming>
          </bpmn:endEvent>
          <bpmn:sequenceFlow id="flow-start" sourceRef="start" targetRef="task"/>
          <bpmn:sequenceFlow id="flow-end" sourceRef="task" targetRef="end"/>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static final String TIMER_PROCESS_XML =
      """
      <?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
          targetNamespace="http://camunda.org/lite" id="lite-timer-definitions">
        <bpmn:process id="lite-timer-process" isExecutable="true">
          <bpmn:startEvent id="start">
            <bpmn:outgoing>flow-start</bpmn:outgoing>
          </bpmn:startEvent>
          <bpmn:serviceTask id="task">
            <bpmn:extensionElements>
              <zeebe:taskDefinition type="lite-timer-job"/>
            </bpmn:extensionElements>
            <bpmn:incoming>flow-start</bpmn:incoming>
            <bpmn:outgoing>flow-timer</bpmn:outgoing>
          </bpmn:serviceTask>
          <bpmn:intermediateCatchEvent id="timer">
            <bpmn:incoming>flow-timer</bpmn:incoming>
            <bpmn:outgoing>flow-end</bpmn:outgoing>
            <bpmn:timerEventDefinition>
              <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">PT2S</bpmn:timeDuration>
            </bpmn:timerEventDefinition>
          </bpmn:intermediateCatchEvent>
          <bpmn:endEvent id="end">
            <bpmn:incoming>flow-end</bpmn:incoming>
          </bpmn:endEvent>
          <bpmn:sequenceFlow id="flow-start" sourceRef="start" targetRef="task"/>
          <bpmn:sequenceFlow id="flow-timer" sourceRef="task" targetRef="timer"/>
          <bpmn:sequenceFlow id="flow-end" sourceRef="timer" targetRef="end"/>
        </bpmn:process>
      </bpmn:definitions>
      """;

  private static int port;
  private static Path dataDir;
  private static CamundaLite.CamundaLiteRuntime runtime;
  private static ZeebeClient client;

  @BeforeAll
  static void startLiteRuntime() throws Exception {
    port = freePort();
    dataDir = Files.createTempDirectory("camunda-lite-test-");
    runtime = CamundaLite.start(port, dataDir);
    client =
        ZeebeClient.newClientBuilder()
            .gatewayAddress("127.0.0.1:" + port)
            .usePlaintext()
            .build();
  }

  @AfterAll
  static void stopLiteRuntime() {
    if (client != null) {
      client.close();
    }
    if (runtime != null) {
      runtime.close();
    }
  }

  @Test
  @Timeout(120)
  void deployProcessRunInstanceAndCompleteJob() throws Exception {
    client.newDeployCommand()
        .addResourceBytes(PROCESS_XML.getBytes(StandardCharsets.UTF_8), "lite-test-process.bpmn")
        .send()
        .toCompletableFuture()
        .get(30, SECONDS);

    try (var worker =
        client.newWorker()
            .jobType("lite-test-job")
            .handler(
                (jobClient, job) ->
                    jobClient.newCompleteCommand(job).variables(Map.of("done", true)).send())
            .timeout(Duration.ofSeconds(30))
            .open()) {

      final ProcessInstanceResult result =
          client.newCreateInstanceCommand()
              .bpmnProcessId("lite-test-process")
              .latestVersion()
              .withResult()
              .send()
              .toCompletableFuture()
              .get(60, SECONDS);
      assertThat(result.getProcessInstanceKey()).isPositive();
      assertThat(result.getVariablesAsMap()).containsEntry("done", true);
    }
  }

  @Test
  @Timeout(120)
  void timerCatchEventFiresAndFinishesInstance() throws Exception {
    client.newDeployCommand()
        .addResourceBytes(
            TIMER_PROCESS_XML.getBytes(StandardCharsets.UTF_8), "lite-timer-process.bpmn")
        .send()
        .toCompletableFuture()
        .get(30, SECONDS);

    try (var worker =
        client.newWorker()
            .jobType("lite-timer-job")
            .handler(
                (jobClient, job) ->
                    jobClient.newCompleteCommand(job).variables(Map.of("done", true)).send())
            .timeout(Duration.ofSeconds(30))
            .open()) {

      final var startedAt = System.nanoTime();
      final ProcessInstanceResult result =
          client.newCreateInstanceCommand()
              .bpmnProcessId("lite-timer-process")
              .latestVersion()
              .withResult()
              .send()
              .toCompletableFuture()
              .get(60, SECONDS);
      final var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

      assertThat(result.getProcessInstanceKey()).isPositive();
      assertThat(result.getVariablesAsMap()).containsEntry("done", true);
      // the instance must have waited for the PT2S timer before finishing
      assertThat(elapsed).isGreaterThanOrEqualTo(Duration.ofSeconds(2));
    }
  }

  private static int freePort() throws Exception {
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
