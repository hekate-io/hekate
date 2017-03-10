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

package io.hekate.spring.boot;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.network.NetworkService;
import io.hekate.network.address.AddressSelector;
import io.hekate.network.address.DefaultAddressSelector;
import io.hekate.network.address.DefaultAddressSelectorConfig;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HekateConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    static class DefaultTestConfig {
        @Autowired
        private Hekate node;

        @Autowired
        private CodecService codecService;

        @Autowired
        private NetworkService networkService;
    }

    @EnableAutoConfiguration
    static class DisableTestConfig {
        @Autowired(required = false)
        private Hekate node;
    }

    @EnableAutoConfiguration
    static class AddressSelectorConfigTestConfig extends HekateTestConfigBase {
        @Bean
        public DefaultAddressSelectorConfig addressSelectorConfig() {
            return new DefaultAddressSelectorConfig().withInterfaceNotMatch("test");
        }
    }

    @EnableAutoConfiguration
    static class AddressSelectorTestConfig extends HekateTestConfigBase {
        public static class TestSelector implements AddressSelector {
            @Override
            public InetAddress select(InetAddress bindAddress) throws HekateException {
                try {
                    return InetAddress.getLocalHost();
                } catch (UnknownHostException e) {
                    throw new HekateException("Failed to select address.", e);
                }
            }
        }

        @Bean
        public AddressSelector addressSelector() {
            return new TestSelector();
        }
    }

    @Test
    public void testDefault() {
        registerAndRefresh(DefaultTestConfig.class);

        Hekate node = getNode();

        assertNotNull(node);
        assertSame(Hekate.State.UP, node.getState());

        DefaultTestConfig app = get(DefaultTestConfig.class);

        assertNotNull(app.node);
        assertNotNull(app.codecService);
        assertNotNull(app.networkService);
    }

    @Test
    public void testDisabled() {
        registerAndRefresh(DisableTestConfig.class);

        assertTrue(getContext().getBeansOfType(Hekate.class).isEmpty());
        assertTrue(getContext().getBeansOfType(ServiceFactory.class).isEmpty());
        assertTrue(getContext().getBeansOfType(Service.class).isEmpty());
        assertTrue(getContext().getBeansOfType(DefaultAddressSelectorConfig.class).isEmpty());
        assertTrue(getContext().getBeansOfType(DefaultAddressSelector.class).isEmpty());
        assertTrue(getContext().getBeansOfType(CodecFactory.class).isEmpty());

        assertNull(get(DisableTestConfig.class).node);
    }

    @Test
    public void testAddressSelectorConfig() {
        registerAndRefresh(AddressSelectorConfigTestConfig.class);

        assertEquals("test", get(DefaultAddressSelector.class).getInterfaceNotMatch());
    }

    @Test
    public void testAddressSelector() {
        registerAndRefresh(AddressSelectorTestConfig.class);

        assertTrue(get(AddressSelector.class) instanceof AddressSelectorTestConfig.TestSelector);
    }
}
