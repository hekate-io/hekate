package io.hekate.spring.boot;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.internal.util.ErrorUtils;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@RunWith(SpringRunner.class)
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(classes = HekateDeferredJoinOffTest.DeferredJoinOffApp.class, webEnvironment = DEFINED_PORT)
@TestPropertySource(properties = "hekate.deferred-join=false")
public class HekateDeferredJoinOffTest extends HekateTestBase {
    @EnableHekate
    @Configuration
    @EnableAutoConfiguration
    static class DeferredJoinOffApp implements Hekate.LifecycleListener {
        private volatile Throwable error;

        @Override
        public void onStateChanged(Hekate changed) {
            if (changed.state() == Hekate.State.JOINING) {
                try (Socket socket = new Socket()) {
                    // Should not be able to connect to the local HTTP server (application is still initializing).
                    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 8080));
                } catch (Throwable e) {
                    error = e;
                }
            }
        }
    }

    @Autowired
    private DeferredJoinOffApp app;

    @Test
    public void test() throws Exception {
        Throwable err = app.error;

        assertNotNull(err);
        assertEquals(ErrorUtils.stackTrace(err), ConnectException.class, err.getClass());
    }

    @Override
    protected void assertAllThreadsStopped() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }
}
