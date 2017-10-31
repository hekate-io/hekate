package io.hekate.spring.boot;

import io.hekate.core.Hekate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import static org.junit.Assert.assertEquals;

public class HekateDeferredJoinTest extends HekateAutoConfigurerTestBase {
    static class TestListener implements Hekate.LifecycleListener {
        private List<String> events = Collections.synchronizedList(new ArrayList<>());

        @EventListener(ApplicationReadyEvent.class)
        public void onAppReady() {
            events.add("app-ready");
        }

        @Override
        public void onStateChanged(Hekate changed) {
            if (changed.state() == Hekate.State.JOINING) {
                events.add("joining");
            }
        }
    }

    @EnableAutoConfiguration
    public static class TestDeferredJoinConfig extends HekateTestConfigBase {
        @Bean
        public TestListener testListener() {
            return new TestListener();
        }
    }

    @Test
    public void testManual() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.deferred-join=true",
            "hekate.deferred-join-condition=manual"
        }, TestDeferredJoinConfig.class);

        TestListener listener = get(TestListener.class);

        assertEquals(1, listener.events.size());
        assertEquals("app-ready", listener.events.get(0));
    }

    @Test
    public void testDeferred() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.deferred-join=true",
            "hekate.deferred-join-condition=app-ready"
        }, TestDeferredJoinConfig.class);

        TestListener listener = get(TestListener.class);

        assertEquals(2, listener.events.size());
        assertEquals("app-ready", listener.events.get(0));
        assertEquals("joining", listener.events.get(1));
    }

    @Test
    public void testNonDeferred() throws Exception {
        registerAndRefresh(TestDeferredJoinConfig.class);

        TestListener listener = get(TestListener.class);

        assertEquals(2, listener.events.size());
        assertEquals("joining", listener.events.get(0));
        assertEquals("app-ready", listener.events.get(1));
    }
}
