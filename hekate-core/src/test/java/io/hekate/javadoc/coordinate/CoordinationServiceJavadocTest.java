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

package io.hekate.javadoc.coordinate;

import io.hekate.HekateNodeTestBase;
import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationProcess;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.coordinate.CoordinatorContext;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.internal.HekateTestNode;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class CoordinationServiceJavadocTest extends HekateNodeTestBase {
    // Start:handler
    public static class ExampleHandler implements CoordinationHandler {
        private final ReentrantLock dataLock = new ReentrantLock();

        @Override
        public void prepare(CoordinationContext ctx) {
            if (ctx.isCoordinator()) {
                System.out.println("Prepared to coordinate " + ctx.members());
            } else {
                System.out.println("Prepared to await for coordination from " + ctx.coordinator());
            }
        }

        @Override
        public void coordinate(CoordinatorContext ctx) {
            System.out.println("Coordinating " + ctx.members());

            // Ask all members to acquire their local locks.
            ctx.broadcast("lock", lockResponses -> {
                // Got lock confirmations from all members.
                // Ask all members to execute some application specific logic while lock is held.
                ctx.broadcast("execute", executeResponses -> {
                    // All members finished executing their tasks.
                    // Ask all members to release their locks.
                    ctx.broadcast("unlock", unlockResponses -> {
                        // All locks released. Coordination completed.
                        System.out.println("Done coordinating.");

                        ctx.complete();
                    });
                });
            });
        }

        @Override
        public void process(CoordinationRequest request, CoordinationContext ctx) {
            String msg = request.get(String.class);

            switch (msg) {
                case "lock": {
                    if (!dataLock.isHeldByCurrentThread()) {
                        dataLock.lock();
                    }

                    break;
                }
                case "execute": {
                    assert dataLock.isHeldByCurrentThread();

                    // ...perform some actions while all members are holding their locks...

                    break;
                }
                case "unlock": {
                    if (dataLock.isHeldByCurrentThread()) {
                        dataLock.unlock();
                    }

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unsupported messages: " + msg);
                }
            }

            // Send confirmation back to the coordinator.
            request.reply("ok");
        }

        @Override
        public void terminate() {
            // Make sure that lock is released if local node gets stopped
            if (dataLock.isHeldByCurrentThread()) {
                dataLock.unlock();
            }
        }
    }
    // End:handler

    @Test
    public void exampleCoordinationService() throws Exception {
        // Start:configure
        // Prepare service factory.
        CoordinationServiceFactory factory = new CoordinationServiceFactory()
            // Register coordination process.
            .withProcess(new CoordinationProcessConfig()
                // Process name.
                .withName("example.process")
                // Coordination handler.
                .withHandler(new ExampleHandler())
            );

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();

        // Access the service.
        CoordinationService coordination = hekate.coordination();
        // End:configure

        assertNotNull(coordination);

        // Start:future
        // Get coordination process (or wait up to 3 seconds for initial coordination to be completed).
        CoordinationProcess process = hekate.coordination()
            .futureOf("example.process")
            .get(AWAIT_TIMEOUT, TimeUnit.SECONDS);

        System.out.println("Coordination completed for " + process.name());
        // End:future

        hekate.leave();

        for (int i = 0; i < 3; i++) {
            HekateTestNode node = createNode(c ->
                c.withService(new CoordinationServiceFactory()
                    .withProcess(new CoordinationProcessConfig()
                        .withName("test")
                        .withHandler(new ExampleHandler())
                    )
                )
            ).join();

            get(node.coordination().futureOf("test"));
        }
    }
}
