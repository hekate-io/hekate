import brave.Tracer;
import brave.Tracing;
import io.hekate.election.Candidate;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockServiceFactory;
import io.hekate.messaging.internal.MessagingServiceTestBase;
import io.hekate.messaging.internal.TestChannel;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.trace.zipkin.HekateZipkinPlugin;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

import static org.mockito.Mockito.mock;

public class HekateZipkinPluginTest extends MessagingServiceTestBase {
    private interface Task {
        void execute() throws Exception;
    }

    @Rpc
    public interface TestRpc {
        String say(String name);
    }

    public static class TestRpcImpl implements TestRpc {
        @Override
        public String say(String name) {
            return "Hello " + name;
        }
    }

    private static OkHttpSender sender;

    private static AsyncReporter<Span> reporter;

    private static Tracing tracing;

    public HekateZipkinPluginTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @BeforeClass
    public static void setUpClass() {
        sender = OkHttpSender.create("http://127.0.0.1:9411/api/v2/spans");

        reporter = AsyncReporter.create(sender);

        tracing = Tracing.newBuilder()
            .localServiceName(HekateZipkinPluginTest.class.getSimpleName())
            .spanReporter(reporter)
            .build();

    }

    @AfterClass
    public static void tearDownClass() {
        if (tracing != null) {
            tracing.close();
        }

        if (reporter != null) {
            reporter.close();
        }

        if (sender != null) {
            sender.close();
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ignoreGhostThreads();
    }

    @Override
    public void tearDown() throws Exception {
        // TODO
        sleep(1000);

        super.tearDown();
    }

    @Test
    public void testMessaging() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3,
            c -> c.withReceiver(msg -> {
                brave.Span span = tracing.tracer().nextSpan().name("SomeWork").start();

                try {
                    // TODO
                    sleep(10);
                } finally {
                    span.finish();
                }

                if (msg.mustReply()) {
                    if (msg.isSubscription()) {
                        msg.partialReply("one");
                        msg.partialReply("two");
                        msg.partialReply("three");
                    }

                    msg.reply("ok");
                }
            }),
            boot -> boot.withPlugin(new HekateZipkinPlugin(tracing))
        );

        trace("Request", () -> {
            get(channels.get(0).get().forRemotes().request("request"));
        });

        trace("Send no ack", () -> {
            get(channels.get(0).get().forRemotes().send("send"));
        });

        trace("Send with ack", () -> {
            get(channels.get(0).get().withConfirmReceive(true).forRemotes().send("send"));
        });

        trace("subscribe", () -> {
            get(channels.get(0).get().forRemotes().subscribe("subscribe"));
        });

        trace("aggregate", () -> {
            get(channels.get(0).get().forRemotes().aggregate("aggregate"));
        });
    }

    @Test
    public void testLocks() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, null, boot -> {
                boot.withPlugin(new HekateZipkinPlugin(tracing));
                boot.withService(LockServiceFactory.class, locks ->
                    locks.withRegion(new LockRegionConfig("test"))
                );
            }
        );

        DistributedLock lock = channels.get(0).node().locks().region("test").get("my-lock");

        for (int i = 0; i < 5; i++) {
            trace("Locks", () -> {
                lock.lock();

                try {
                    trace("some work", () -> {
                        sleep(5);
                    });
                } finally {
                    lock.unlock();
                }
            });
        }
    }

    @Test
    public void testRpc() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, null,
            boot -> {
                boot.withPlugin(new HekateZipkinPlugin(tracing));
                boot.withService(RpcServiceFactory.class, rpc ->
                    rpc.withServer(new RpcServerConfig().withHandler(new TestRpcImpl()))
                );
            }
        );

        trace("RPC", () -> {
            TestRpc rpc = channels.get(0).node().rpc().clientFor(TestRpc.class).build();

            say(rpc.say("Test"));
        });
    }

    @Test
    public void testElection() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(3, null,
            boot -> {
                boot.withPlugin(new HekateZipkinPlugin(tracing));
                boot.withService(ElectionServiceFactory.class, election ->
                    election.withCandidate("test-group").withCandidate(mock(Candidate.class))
                );
            });

        channels.forEach(c ->
            c.node().election().leader("test-group").join()
        );
    }

    private void trace(String taskName, Task task) throws Exception {
        brave.Span span = tracing.tracer().nextSpan().name(taskName).start();

        try (Tracer.SpanInScope ignore = tracing.tracer().withSpanInScope(span)) {
            task.execute();
        } finally {
            span.finish();
        }
    }
}
