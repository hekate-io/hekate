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

package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.operation.RequestFuture;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class BackPressureTestBase extends MessagingServiceTestBase {
    public static class BackPressureTestContext {
        private final MessagingTestContext parent;

        private final int lowWatermark;

        private final int highWatermark;

        public BackPressureTestContext(MessagingTestContext parent, int lowWatermark, int highWatermark) {
            this.parent = parent;
            this.lowWatermark = lowWatermark;
            this.highWatermark = highWatermark;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    protected final int lowWatermark;

    protected final int highWatermark;

    public BackPressureTestBase(BackPressureTestContext ctx) {
        super(ctx.parent);

        lowWatermark = ctx.lowWatermark;
        highWatermark = ctx.highWatermark;
    }

    protected MessagingChannelConfig<String> useBackPressure(MessagingChannelConfig<String> cfg) {
        return cfg.withBackPressure(bp -> {
            bp.setOutOverflowPolicy(MessagingOverflowPolicy.FAIL);
            bp.setOutLowWatermark(lowWatermark);
            bp.setOutHighWatermark(highWatermark);
            bp.setInLowWatermark(lowWatermark);
            bp.setInHighWatermark(highWatermark);
        });
    }

    protected boolean isBackPressureEnabled(MessagingChannel<String> channel) {
        // Check that message can't be sent when high watermark reached.
        try {
            get(channel.newRequest("fail-on-back-pressure").submit());

            return false;
        } catch (TimeoutException | InterruptedException e) {
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            return ErrorUtils.isCausedBy(MessageQueueOverflowException.class, e);
        }
    }

    protected void assertBackPressureEnabled(MessagingChannel<String> channel) {
        assertTrue(isBackPressureEnabled(channel));
    }

    protected int getLowWatermarkBounds() {
        return Math.max(1, lowWatermark);
    }

    protected void assertBackPressureOnPartialReply(Message<String> msg) throws Exception {
        assertTrue(msg.mustReply());

        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        msg.partialReply("fail-on-back-pressure", errFuture::complete);

        Throwable err = get(errFuture);

        assertNotNull(err);
        assertTrue(err.toString(), ErrorUtils.isCausedBy(MessageQueueOverflowException.class, err));
    }

    protected List<RequestFuture<String>> requestUpToHighWatermark(MessagingChannel<String> channel) {
        List<RequestFuture<String>> responses = new ArrayList<>();

        // Request up to high watermark in order to enable back pressure.
        for (int i = 0; i < highWatermark; i++) {
            responses.add(channel.forRemotes().newRequest("request-" + i).submit());
        }

        return responses;
    }
}
