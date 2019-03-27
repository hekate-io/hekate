package io.hekate.rpc;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RpcRetryInfoTest extends HekateTestBase {
    @SuppressWarnings("unchecked")
    public static final Class<? extends Throwable>[] EMPTY = new Class[0];

    @Test
    public void testEmpty() {
        List<Class<? extends Throwable>> errors = emptyList();
        String maxAttempts = "";
        String delay = "";
        String maxDelay = "";

        RpcRetryInfo info = buildInfo(maxAttempts, delay, maxDelay, errors);

        assertTrue(info.errors().isEmpty());
        assertFalse(info.maxAttempts().isPresent());
        assertFalse(info.delay().isPresent());
        assertFalse(info.maxDelay().isPresent());
    }

    @Test
    public void testValues() {
        List<Class<? extends Throwable>> errors = Arrays.asList(IOException.class, UncheckedIOException.class);
        String maxAttempts = "1";
        String delay = "2";
        String maxDelay = "3";

        RpcRetryInfo info = buildInfo(maxAttempts, delay, maxDelay, errors);

        assertEquals(errors, info.errors());
        assertEquals(Collections.unmodifiableList(errors).getClass(), info.errors().getClass());
        assertEquals(1, info.maxAttempts().getAsInt());
        assertEquals(2, info.delay().getAsLong());
        assertEquals(3, info.maxDelay().getAsLong());
    }

    @Test
    public void testInvalidValues() {
        expectCause(IllegalArgumentException.class, "RpcRetry#maxAttempts()", () ->
            buildInfo("zzz", "", "", emptyList())
        );

        expectCause(IllegalArgumentException.class, "RpcRetry#delay()", () ->
            buildInfo("", "zzz", "", emptyList())
        );

        expectCause(IllegalArgumentException.class, "RpcRetry#maxDelay()", () ->
            buildInfo("", "", "zzz", emptyList())
        );
    }

    @Test
    public void testToString() {
        RpcRetryInfo info = buildInfo("", "", "", emptyList());

        assertEquals(ToString.format(info), info.toString());
    }

    private RpcRetryInfo buildInfo(
        String maxAttempts,
        String delay,
        String maxDelay,
        List<Class<? extends Throwable>> errors
    ) {
        RpcRetry retry = new RpcRetry() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return RpcRetry.class;
            }

            @Override
            public Class<? extends Throwable>[] errors() {
                return errors.toArray(EMPTY);
            }

            @Override
            public String maxAttempts() {
                return maxAttempts;
            }

            @Override
            public String delay() {
                return delay;
            }

            @Override
            public String maxDelay() {
                return maxDelay;
            }
        };

        return RpcRetryInfo.parse(retry);
    }
}
