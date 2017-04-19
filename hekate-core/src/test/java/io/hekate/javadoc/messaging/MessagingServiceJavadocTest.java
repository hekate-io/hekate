/*
 * Copyright 2017 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.javadoc.messaging;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.broadcast.BroadcastResult;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class MessagingServiceJavadocTest extends HekateNodeTestBase {
    // Start:message_receiver
    public static class ExampleReceiver implements MessageReceiver<String> {
        @Override
        public void receive(Message<String> message) {
            // Get payload.
            String payload = message.get();

            // Check if sender is expecting a response.
            if (message.mustReply()) {
                System.out.println("Received request: " + payload);

                // Send back the response.
                message.reply("...some response...");
            } else {
                // No need to send a response since this is a unidirectional message.
                System.out.println("Received notification: " + payload);
            }
        }
    }
    // End:message_receiver

    @Test
    public void exampleMessageReceiver() throws Exception {
        Hekate hekate = new HekateBootstrap()
            .withService(new MessagingServiceFactory()
                .withChannel(new MessagingChannelConfig<String>()
                    .withName("example")
                    .withReceiver(new ExampleReceiver())))
            .join();

        hekate.messaging().channel("example").aggregate("example message").get();

        hekate.leave();
    }

    @Test
    public void exampleChannel() throws Exception {
        // Start:configure_channel
        // Prepare channel configuration that will support messages of String type (for simplicity).
        MessagingChannelConfig<String> channelCfg = new MessagingChannelConfig<String>()
            .withName("example.channel") // Channel name.
            // Message receiver (optional - if not specified then channel will act as a sender only)
            .withReceiver(msg -> {
                // Get payload.
                String request = msg.get();

                System.out.println("Received request: " + request);

                // Send reply (if required).
                if (msg.mustReply()) {
                    msg.reply(request + " response");
                }
            });

        // Prepare messaging service factory and register channel configuration.
        MessagingServiceFactory factory = new MessagingServiceFactory()
            .withChannel(channelCfg);

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure_channel

        // Start:access
        MessagingService messaging = hekate.messaging();
        // End:access

        // Start:access_channel
        MessagingChannel<String> channel = messaging.channel("example.channel");
        // End:access_channel

        assertNotNull(channel);

        exampleUnicast(hekate);

        broadcastExample(hekate);

        hekate.leave();
    }

    @Test
    public void exampleChannelOptions() throws Exception {
        // Start:channel_options
        // Prepare channel configuration.
        MessagingChannelConfig<String> channelCfg = new MessagingChannelConfig<String>()
            // Channel name.
            .withName("example.channel")
            // Thread pooling options.
            .withNioThreads(2)
            .withWorkerThreads(6);

        // Prepare messaging service factory and register channel configuration.
        MessagingServiceFactory factory = new MessagingServiceFactory()
            .withChannel(channelCfg);

        // ...register service and start node...

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:channel_options

        hekate.leave();
    }

    private void exampleUnicast(Hekate hekate) throws Exception {
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel");

        unicastRequestSyncExample(channel);

        unicastRequestAsyncExample(channel);

        unicastSendSyncExample(channel);

        unicastSendAsyncExample(channel);
    }

    private void unicastSendAsyncExample(MessagingChannel<String> channel) {
        // Start:unicast_send_async
        // Send message to the oldest node in the cluster
        // and process operation result in the asynchronous callback.
        channel.forOldest().send("example message", err -> {
            if (err == null) {
                System.out.println("Message sent.");
            } else {
                System.out.println("Sending failed: " + err);
            }
        });
        // End:unicast_send_async
    }

    private void unicastSendSyncExample(MessagingChannel<String> channel) throws InterruptedException, MessagingFutureException {
        // Start:unicast_send_sync
        // Send message to the oldest node in the cluster
        // and synchronously await for operation result (success/failure).
        channel.forOldest().send("example message").get();
        // End:unicast_send_sync
    }

    private void unicastRequestAsyncExample(MessagingChannel<String> channel) {
        // Start:unicast_request_async
        // Submit request to the oldest node in the cluster
        // and process reply in the asynchronous callback.
        channel.forOldest().request("example request", (err, reply) -> {
            if (err == null) {
                System.out.println("Got reply: " + reply.get());
            } else {
                System.out.println("Request failed: " + err);
            }
        });
        // End:unicast_request_async
    }

    private void unicastRequestSyncExample(MessagingChannel<String> channel)
        throws MessagingFutureException, InterruptedException {
        // Start:unicast_request_sync
        // Execute request to the oldest node in the cluster and synchronously await for reply.
        String reply = channel.forOldest().request("example request").response();
        // End:unicast_request_sync

        assertNotNull(reply);
    }

    private void broadcastExample(Hekate hekate) throws InterruptedException {
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel");

        // Start:aggregate_sync
        try {
            // Submit aggregation request to all remote nodes.
            AggregateResult<String> result = channel.forRemotes().aggregate("example message").get();

            // Iterate over aggregation participants and check their responses.
            result.nodes().forEach(node -> {
                if (result.isSuccess(node)) {
                    System.out.println("Got response from " + node + ": " + result.resultOf(node));
                } else {
                    System.out.println("Partial failure on node  " + node + ": " + result.errorOf(node));
                }
            });
        } catch (MessagingFutureException e) {
            System.out.println("Aggregation failed: " + e.getCause());
        }
        // End:aggregate_sync

        // Start:aggregate_async
        // Asynchronously submit aggregation request to all remote nodes.
        channel.forRemotes().aggregate("example message", (err, result) -> {
            if (err == null) {
                // Iterate over aggregation participants and check their responses.
                result.nodes().forEach(node -> {
                    if (result.isSuccess(node)) {
                        System.out.println("Got response from " + node + ": " + result.resultOf(node));
                    } else {
                        System.out.println("Partial failure on node  " + node + ": " + result.errorOf(node));
                    }
                });
            } else {
                System.out.println("Aggregation failed: " + err);
            }
        });
        // End:aggregate_async

        // Start:broadcast_sync
        try {
            // Broadcast message to all remote nodes.
            BroadcastResult<String> result = channel.forRemotes().broadcast("example message").get();

            // Iterate over participants and check operation results.
            result.nodes().forEach(node -> {
                if (result.isSuccess(node)) {
                    System.out.println("Successfully sent to " + node);
                } else {
                    System.out.println("Partial failure on node " + node + ": " + result.errorOf(node));
                }
            });
        } catch (MessagingFutureException e) {
            System.out.println("Broadcast failed: " + e.getCause());
        }
        // End:broadcast_sync

        // Start:broadcast_async
        // Asynchronously broadcast message to all remote nodes.
        channel.forRemotes().broadcast("example message", (err, result) -> {
            if (err == null) {
                // Iterate over participants and check operation results.
                result.nodes().forEach(node -> {
                    if (result.isSuccess(node)) {
                        System.out.println("Successfully sent to " + node);
                    } else {
                        System.out.println("Partial failure on node " + node + ": " + result.errorOf(node));
                    }
                });
            } else {
                System.out.println("Broadcast failed: " + err);
            }
        });
        // End:broadcast_async
    }
}
