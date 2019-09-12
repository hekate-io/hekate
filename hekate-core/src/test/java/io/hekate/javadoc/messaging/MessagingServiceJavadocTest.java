/*
 * Copyright 2019 The Hekate Project
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
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.RequestFuture;
import io.hekate.messaging.operation.SendFuture;
import io.hekate.messaging.operation.SubscribeFuture;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class MessagingServiceJavadocTest extends HekateNodeTestBase {
    // Start:message_receiver
    public static class ExampleReceiver implements MessageReceiver<String> {
        @Override
        public void receive(Message<String> msg) {
            // Get payload.
            String payload = msg.payload();

            // Check if the sender expects a response.
            if (msg.mustReply()) {
                System.out.println("Request received: " + payload);

                // Send back a response.
                msg.reply("...some response...");
            } else {
                // No need to send a response since this is a unidirectional message.
                System.out.println("Notification received: " + payload);
            }
        }
    }
    // End:message_receiver

    @Test
    public void exampleMessageReceiver() throws Exception {
        Hekate hekate = new HekateBootstrap()
            .withService(new MessagingServiceFactory()
                .withChannel(MessagingChannelConfig.of(String.class)
                    .withName("example")
                    .withReceiver(new ExampleReceiver())))
            .join();

        hekate.messaging().channel("example", String.class).newAggregate("example message").submit().get();

        hekate.leave();
    }

    @Test
    public void exampleChannel() throws Exception {
        // Start:configure_channel
        // Configure channel that will support messages of String type (for simplicity).
        MessagingChannelConfig<String> channelCfg = MessagingChannelConfig.of(String.class)
            .withName("example.channel") // Channel name.
            // Message receiver (optional - if not specified then channel will act as a sender only)
            .withReceiver(msg -> {
                System.out.println("Request received: " + msg.payload());

                // Reply (if this is a request and not a unidirectional notification).
                if (msg.mustReply()) {
                    msg.reply("some response");
                }
            });

        // Start node.
        Hekate hekate = new HekateBootstrap()
            // Register channel to the messaging service.
            .withService(MessagingServiceFactory.class, messaging ->
                messaging.withChannel(channelCfg)
            )
            .join();
        // End:configure_channel

        // Start:access_channel
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);
        // End:access_channel

        assertNotNull(channel);

        exampleUnicast(hekate);

        broadcastExample(hekate);

        hekate.leave();
    }

    private void exampleUnicast(Hekate hekate) throws Exception {
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        unicastRequestSyncExample(channel);

        unicastRequestAsyncExample(channel);

        unicastSubscribeSyncExample(channel);

        unicastSendSyncExample(channel);

        unicastSendAsyncExample(channel);

        sendOperationExample(hekate);

        requestOperationExample(hekate);

        broadcastOperationExample(hekate);

        aggregateOperationExample(hekate);

        subscribeOperationExample(hekate);
    }

    private void sendOperationExample(Hekate hekate) throws Exception {
        // Start:send_operation
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        SendFuture future = channel.newSend("some-message") // Some dummy message.
            .withAckMode(AckMode.REQUIRED) // Set acknowledgement mode.
            .withTimeout(3, TimeUnit.SECONDS) // Timeout.
            .withAffinity("100500") // Some dummy affinity key.
            .withRetry(retry -> retry
                .whileError(err -> err.isCausedBy(IOException.class)) // Only if I/O error.
                .withFixedDelay(100) // Delay between retries.
                .maxAttempts(3) // Retry up to 3 times.
                .alwaysReRoute() // Retry on different nodes.
            )
            .submit(); // Asynchronously execute the operation.

        future.join(); // Await for confirmation.
        // End:send_operation
    }

    private void requestOperationExample(Hekate hekate) throws Exception {
        // Start:request_operation
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        RequestFuture<String> future = channel.newRequest("some-message") // Some dummy message.
            .withTimeout(3, TimeUnit.SECONDS) // Timeout.
            .withAffinity("100500") // Some dummy affinity key.
            .withRetry(retry -> retry
                .whileError(err -> err.isCausedBy(IOException.class)) // Only if I/O error.
                .withFixedDelay(100) // Delay between retries.
                .maxAttempts(3) // Retry up to 3 times.
                .alwaysReRoute() // Retry on different nodes.
            )
            .submit(); // Asynchronously execute the operation.

        // Await and print the response.
        System.out.println("Response: " + future.result());
        // End:request_operation
    }

    private void subscribeOperationExample(Hekate hekate) throws Exception {
        // Start:subscribe_operation
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        SubscribeFuture<String> future = channel.newSubscribe("some-message") // Some dummy message.
            .withTimeout(3, TimeUnit.SECONDS) // Timeout.
            .withAffinity("100500") // Some dummy affinity key.
            .withRetry(retry -> retry
                .whileError(err -> err.isCausedBy(IOException.class)) // Only if I/O error.
                .withFixedDelay(100) // Delay between retries.
                .maxAttempts(3) // Retry up to 3 times.
                .alwaysReRoute() // Retry on different nodes.
            )
            // Execute and listen for responses.
            .submit((err, rsp) -> {
                if (rsp.isLastPart()) {
                    System.out.println("Got the last response chunk: " + rsp.payload());
                } else {
                    System.out.println("Got a response chunk: " + rsp.payload());
                }
            });

        // Await and print the last response.
        System.out.println("Last response: " + future.get());
        // End:subscribe_operation
    }

    private void broadcastOperationExample(Hekate hekate) throws Exception {
        // Start:broadcast_operation
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        BroadcastFuture<String> future = channel.newBroadcast("some-message") // Some dummy message.
            .withAckMode(AckMode.REQUIRED) // Set acknowledgement mode.
            .withTimeout(3, TimeUnit.SECONDS) // Timeout.
            .withAffinity("100500") // Some dummy affinity key.
            .withRetry(retry -> retry
                .whileError(err -> err.isCausedBy(IOException.class)) // Only if I/O error.
                .withFixedDelay(100) // Delay between retries.
                .maxAttempts(3) // Retry up to 3 times.
            )
            .submit(); // Asynchronously execute the operation.

        future.join(); // Await for confirmations.
        // End:broadcast_operation
    }

    private void aggregateOperationExample(Hekate hekate) throws Exception {
        // Start:aggregate_operation
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        AggregateFuture<String> future = channel.newAggregate("some-message") // Some dummy message.
            .withTimeout(3, TimeUnit.SECONDS) // Timeout.
            .withAffinity("100500") // Some dummy affinity key.
            .withRetry(retry -> retry
                .whileError(err -> err.isCausedBy(IOException.class)) // Only if I/O error.
                .withFixedDelay(100) // Delay between retries.
                .maxAttempts(3) // Retry up to 3 times.
            )
            .submit(); // Asynchronously execute the operation.

        // Await and print results.
        System.out.println("Results: " + future.results());
        // End:aggregate_operation
    }

    private void unicastSendAsyncExample(MessagingChannel<String> channel) {
        // Start:unicast_send_async
        // Send message and process results in the asynchronous callback.
        channel.newSend("example message").submit(err -> {
            if (err == null) {
                System.out.println("Message sent.");
            } else {
                System.out.println("Sending failed: " + err);
            }
        });
        // End:unicast_send_async
    }

    private void unicastSendSyncExample(MessagingChannel<String> channel) throws Exception {
        // Start:unicast_send_sync
        // Send message and synchronously await for the operation's result.
        channel.newSend("example message").sync();
        // End:unicast_send_sync
    }

    private void unicastRequestAsyncExample(MessagingChannel<String> channel) {
        // Start:unicast_request_async
        // Submit request and process response in the asynchronous callback.
        channel.newRequest("example request").submit((err, rsp) -> {
            if (err == null) {
                System.out.println("Got response: " + rsp.payload());
            } else {
                System.out.println("Request failed: " + err);
            }
        });
        // End:unicast_request_async
    }

    private void unicastRequestSyncExample(MessagingChannel<String> channel)
        throws MessagingFutureException, InterruptedException {
        // Start:unicast_request_sync
        // Submit request and synchronously await for the response.
        String response = channel.newRequest("example request").response();
        // End:unicast_request_sync

        assertNotNull(response);
    }

    private void unicastSubscribeSyncExample(MessagingChannel<String> channel) {
        // Start:unicast_subscribe_async
        // Submit request and synchronously await for the response.
        channel.newSubscribe("example request").submit((err, rsp) -> {
            if (rsp.isLastPart()) {
                System.out.println("Done: " + rsp.payload());
            } else {
                System.out.println("Update: " + rsp.payload());
            }
        });
        // End:unicast_subscribe_async
    }

    private void broadcastExample(Hekate hekate) throws Exception {
        MessagingChannel<String> channel = hekate.messaging().channel("example.channel", String.class);

        // Start:aggregate_sync
        // Submit aggregation request.
        channel.newAggregate("example message").results().forEach(rslt ->
            System.out.println("Got results: " + rslt)
        );
        // End:aggregate_sync

        // Start:aggregate_async
        // Asynchronously submit aggregation request.
        channel.newAggregate("example message").submit((err, rslts) -> {
            if (err == null) {
                rslts.forEach(rslt ->
                    System.out.println("Got results: " + rslt)
                );
            } else {
                System.out.println("Aggregation failure: " + err);
            }
        });
        // End:aggregate_async

        // Start:broadcast_sync
        // Broadcast message.
        channel.newBroadcast("example message").sync();
        // End:broadcast_sync

        // Start:broadcast_async
        // Asynchronously broadcast message.
        channel.newBroadcast("example message").submit((err, rslt) -> {
            if (err == null) {
                System.out.println("Broadcast success.");
            } else {
                System.out.println("Broadcast failure: " + err);
            }
        });
        // End:broadcast_async
    }
}
