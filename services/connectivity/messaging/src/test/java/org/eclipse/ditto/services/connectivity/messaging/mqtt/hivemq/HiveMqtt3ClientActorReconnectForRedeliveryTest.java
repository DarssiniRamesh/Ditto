/*
 * Copyright (c) 2026 Contributors
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.services.connectivity.messaging.mqtt.hivemq;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.services.connectivity.messaging.TestConstants.Authorization.AUTHORIZATION_CONTEXT;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.ditto.model.base.headers.DittoHeaders;
import org.eclipse.ditto.model.connectivity.Connection;
import org.eclipse.ditto.model.connectivity.ConnectivityModelFactory;
import org.eclipse.ditto.model.connectivity.ConnectivityStatus;
import org.eclipse.ditto.model.connectivity.ConnectionId;
import org.eclipse.ditto.model.connectivity.ConnectionType;
import org.eclipse.ditto.model.connectivity.Source;
import org.eclipse.ditto.model.connectivity.Target;
import org.eclipse.ditto.model.connectivity.Topic;
import org.eclipse.ditto.services.connectivity.messaging.TestConstants;
import org.eclipse.ditto.services.models.connectivity.BaseClientState;
import org.eclipse.ditto.signals.commands.connectivity.modify.OpenConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;

/**
 * P0: Resilience tests for Ditto MQTT connectivity reconnect/redelivery flows.
 *
 * This test focuses on the HiveMQ MQTT3 client actor and verifies that the reconnect-for-redelivery trigger
 * results in a consumer-client restart when a separate publisher client is enabled.
 */
public final class HiveMqtt3ClientActorReconnectForRedeliveryTest {

    private static final Status.Success CONNECTED_SUCCESS = new Status.Success(BaseClientState.CONNECTED);

    private static final TestConstants.FreePort FREE_PORT = new TestConstants.FreePort();

    private static final Target TARGET = ConnectivityModelFactory.newTargetBuilder()
            .address("target")
            .authorizationContext(AUTHORIZATION_CONTEXT)
            .qos(1)
            .topics(Topic.TWIN_EVENTS)
            .build();

    private static final Source MQTT_SOURCE = ConnectivityModelFactory.newSourceBuilder()
            .authorizationContext(AUTHORIZATION_CONTEXT)
            .index(1)
            .consumerCount(1)
            .address("source")
            .qos(1)
            .build();

    @ClassRule
    public static final org.eclipse.ditto.services.connectivity.messaging.mqtt.MqttServerRule MQTT_SERVER =
            new org.eclipse.ditto.services.connectivity.messaging.mqtt.MqttServerRule(FREE_PORT.getPort());

    private ActorSystem actorSystem;
    private ConnectionId connectionId;
    private Connection connection;
    private MockHiveMqtt3ClientFactory mockClientFactory;

    @Before
    public void setUp() {
        actorSystem = ActorSystem.create("AkkaReconnectForRedeliveryTestSystem", TestConstants.CONFIG);
        connectionId = TestConstants.createRandomConnectionId();

        final String serverHost = "tcp://localhost:" + FREE_PORT.getPort();

        final Map<String, String> specificConfig = new HashMap<>();
        specificConfig.put("reconnectForRedelivery", "true");
        specificConfig.put("separatePublisherClient", "true");
        // Use 0s to force the actor's internal lower bound (>= 1s) logic for reconnect scheduling.
        specificConfig.put("reconnectForRedeliveryDelay", "0s");

        connection = ConnectivityModelFactory.newConnectionBuilder(connectionId, ConnectionType.MQTT,
                ConnectivityStatus.CLOSED, serverHost)
                .sources(singletonList(MQTT_SOURCE))
                .targets(singletonList(TARGET))
                .specificConfig(specificConfig)
                .failoverEnabled(true)
                .build();

        // Init Mqtt3Client mock to avoid slow class initialization in HiveMQ client library (same reason as existing tests).
        org.mockito.Mockito.mock(com.hivemq.client.mqtt.mqtt3.Mqtt3Client.class);
        mockClientFactory = new MockHiveMqtt3ClientFactory();
    }

    @After
    public void tearDown() {
        if (actorSystem != null) {
            TestKit.shutdownActorSystem(actorSystem);
        }
    }

    @Test
    public void reconnectForRedeliveryTriggersConsumerClientRestartWhenSeparatePublisherClientEnabled() {
        new TestKit(actorSystem) {{
            final TestProbe controlProbe = TestProbe.apply(actorSystem);

            final Props props = HiveMqtt3ClientActor.props(connection, getRef(), getRef(),
                    mockClientFactory.withTestProbe(getRef()));
            final ActorRef underTest = actorSystem.actorOf(props, "mqttClientActor-reconnectForRedelivery");

            underTest.tell(OpenConnection.of(connectionId, DittoHeaders.empty()), controlProbe.ref());
            controlProbe.expectMsg(CONNECTED_SUCCESS);

            // At startup we expect:
            //  - 1 subscriber client
            //  - 1 publisher client (because separatePublisherClient=true)
            assertThat(mockClientFactory.getCreatedClientCount()).isGreaterThanOrEqualTo(2);

            final int clientsBefore = mockClientFactory.getCreatedClientCount();

            // Trigger reconnect-for-redelivery. The actor schedules a DO_RECONNECT after a delay with lower bound 1s.
            underTest.tell(AbstractMqttClientActor.Control.RECONNECT_CONSUMER_CLIENT, getRef());

            // Ensure no immediate restart happened (lower bound should prevent <1s).
            Thread.sleep(300);
            assertThat(mockClientFactory.getCreatedClientCount()).isEqualTo(clientsBefore);

            // Wait long enough for the scheduled DO_RECONNECT to execute and create a new subscriber client.
            // We allow up to 3 seconds to avoid flakiness in slower CI.
            final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(3);
            while (System.currentTimeMillis() < deadlineMs
                    && mockClientFactory.getCreatedClientCount() == clientsBefore) {
                Thread.sleep(100);
            }

            assertThat(mockClientFactory.getCreatedClientCount()).isGreaterThan(clientsBefore);
        }};
    }
}
