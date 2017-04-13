package io.hekate.messaging.internal;

import java.util.Collection;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;

public abstract class BackPressureParametrizedTestBase extends BackPressureTestBase {
    public BackPressureParametrizedTestBase(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<BackPressureTestContext> getBackPressureTestContexts() {
        return getMessagingServiceTestContexts().stream().flatMap(ctx ->
            Stream.of(
                new BackPressureTestContext(ctx, 0, 1),
                new BackPressureTestContext(ctx, 2, 4)
            ))
            .collect(toList());
    }
}
