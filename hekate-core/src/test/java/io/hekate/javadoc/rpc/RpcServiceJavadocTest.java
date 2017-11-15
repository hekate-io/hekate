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

package io.hekate.javadoc.rpc;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class RpcServiceJavadocTest extends HekateNodeTestBase {
    // Start:interface
    @Rpc
    interface SomeRpcService {
        String helloWorld(String name);
    }
    // End:interface

    // Start:impl
    public static class SomeRpcServiceImpl implements SomeRpcService {
        @Override
        public String helloWorld(String name) {
            return "Hello " + name;
        }
    }
    // End:impl

    @Test
    public void exampleRpcService() throws Exception {
        // Start:configure
        // Prepare service factory.
        RpcServiceFactory factory = new RpcServiceFactory()
            .withWorkerThreads(8)
            // Register some RPC server object for remote access.
            .withServer(new RpcServerConfig()
                .withHandler(new SomeRpcServiceImpl())
            );

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        RpcService rpc = hekate.rpc();
        // End:access

        SomeRpcService client = hekate.rpc().clientFor(SomeRpcService.class)
            // Setup some timeout for remote calls (optional).
            .withTimeout(3, TimeUnit.SECONDS)
            .build();

        System.out.println(client.helloWorld("Hekate"));

        assertNotNull(rpc);

        hekate.leave();
    }
}
