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

package io.hekate.spring.boot.rpc;

import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HekateRpcServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @Rpc
    public interface TestRpc {
        int someMethod(int arg);
    }

    public static class TestRpcImpl1 implements TestRpc {
        @Override
        public int someMethod(int arg) {
            return arg + arg;
        }
    }

    public static class TestRpcImpl2 implements TestRpc {
        @Override
        public int someMethod(int arg) {
            return arg * arg;
        }
    }

    @EnableAutoConfiguration
    public static class RpcTestConfig extends HekateTestConfigBase {
        private static class InnerBean {
            @InjectRpcClient(tag = "rpc-1")
            private TestRpc rpc1;

            @InjectRpcClient(tag = "rpc-2")
            private TestRpc rpc2;

            @InjectRpcClient
            private TestRpc rpc3;
        }

        @InjectRpcClient(tag = "rpc-1")
        private TestRpc rpc1;

        @InjectRpcClient(tag = "rpc-2")
        private TestRpc rpc2;

        @InjectRpcClient
        private TestRpc rpc3;

        @Bean
        public InnerBean innerBean() {
            return new InnerBean();
        }

        @Bean
        public RpcServerConfig rpcServer1() {
            return new RpcServerConfig()
                .withTag("rpc-1")
                .withHandler(new TestRpcImpl1());
        }

        @Bean
        public RpcServerConfig rpcServer2() {
            return new RpcServerConfig()
                .withTag("rpc-2")
                .withHandler(new TestRpcImpl2());
        }

        @Bean
        public RpcServerConfig rpcServer3() {
            return new RpcServerConfig().withHandler(new TestRpcImpl2());
        }
    }

    @Test
    public void test() {
        registerAndRefresh(RpcTestConfig.class);

        assertNotNull(get("rpcService", RpcService.class));

        TestRpc rpc1 = get(RpcTestConfig.class).rpc1;
        TestRpc rpc2 = get(RpcTestConfig.class).rpc2;
        TestRpc rpc3 = get(RpcTestConfig.class).rpc3;
        TestRpc innerRpc1 = get(RpcTestConfig.InnerBean.class).rpc1;
        TestRpc innerRpc2 = get(RpcTestConfig.InnerBean.class).rpc2;
        TestRpc innerRpc3 = get(RpcTestConfig.InnerBean.class).rpc3;

        assertNotNull(rpc1);
        assertNotNull(rpc2);
        assertNotNull(rpc3);
        assertNotNull(innerRpc1);
        assertNotNull(innerRpc2);
        assertNotNull(innerRpc3);

        assertEquals(8, rpc1.someMethod(4));
        assertEquals(16, rpc2.someMethod(4));
        assertEquals(16, rpc3.someMethod(4));
        assertEquals(8, innerRpc1.someMethod(4));
        assertEquals(16, innerRpc2.someMethod(4));
        assertEquals(16, innerRpc3.someMethod(4));

        class TestAutowire {
            @Autowired
            private RpcService rpcService;
        }

        assertNotNull(autowire(new TestAutowire()).rpcService);
    }
}
