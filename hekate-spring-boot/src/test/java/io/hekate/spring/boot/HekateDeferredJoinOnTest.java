package io.hekate.spring.boot;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.DEFINED_PORT;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@RunWith(SpringRunner.class)
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(classes = HekateDeferredJoinOnTest.DeferredJoinOnApp.class, webEnvironment = DEFINED_PORT)
@TestPropertySource(properties = "hekate.deferred-join=true")
public class HekateDeferredJoinOnTest extends HekateTestBase {
    @EnableHekate
    @Configuration
    @EnableAutoConfiguration
    static class DeferredJoinOnApp implements Hekate.LifecycleListener {
        private volatile boolean checked;

        private volatile Throwable unexpectedErr;

        @Override
        public void onStateChanged(Hekate changed) {
            if (changed.state() == Hekate.State.JOINING) {
                checked = true;

                try (Socket socket = new Socket()) {
                    // Should be able to connect to the local HTTP server with deferred join (application must be already initialized).
                    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 8080));
                } catch (Throwable e) {
                    unexpectedErr = e;
                }
            }
        }
    }

    @Autowired
    private DeferredJoinOnApp app;

    @Test
    public void test() throws Exception {
        assertTrue(app.checked);
        assertNull(app.unexpectedErr);
    }

    @Override
    protected void assertAllThreadsStopped() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }
}
