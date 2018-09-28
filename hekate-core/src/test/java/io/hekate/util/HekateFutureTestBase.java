package io.hekate.util;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateFutureException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class HekateFutureTestBase<V, F extends HekateFuture<V, F>, E extends HekateFutureException> extends HekateTestBase {
    protected abstract F createFuture();

    protected abstract Class<E> errorType();

    protected abstract V createValue() throws Exception;

    @Test
    public void testGet() throws Exception {
        F fut = createFuture();
        V val = createValue();

        fut.complete(val);

        assertSame(val, fut.get());
        assertTrue(fut.isSuccess());
    }

    @Test
    public void testGetWithTimeout() throws Exception {
        F fut = createFuture();
        V val = createValue();

        fut.complete(val);

        assertSame(val, fut.get());
        assertTrue(fut.isSuccess());

        assertSame(val, fut.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testGetUninterruptedly() throws Exception {
        F fut = createFuture();
        V val = createValue();

        fut.complete(val);

        assertSame(val, fut.get());
        assertTrue(fut.isSuccess());

        assertSame(val, fut.getUninterruptedly());
    }

    @Test
    public void testGetError() throws Exception {
        F fut = createFuture();

        fut.completeExceptionally(TEST_ERROR);

        try {
            fut.get();

            fail("Error was expected.");
        } catch (Exception err) {
            assertFalse(fut.isSuccess());

            assertEquals(errorType(), err.getClass());
            assertSame(TEST_ERROR, err.getCause());
        }
    }

    @Test
    public void testGetWithTimeoutError() throws Exception {
        F fut = createFuture();

        fut.completeExceptionally(TEST_ERROR);

        try {
            fut.get(1, TimeUnit.MILLISECONDS);

            fail("Error was expected.");
        } catch (Exception err) {
            assertFalse(fut.isSuccess());

            assertEquals(errorType(), err.getClass());
            assertSame(TEST_ERROR, err.getCause());
        }
    }

    @Test
    public void testGetUninterruptedlyError() throws Exception {
        F fut = createFuture();

        fut.completeExceptionally(TEST_ERROR);

        try {
            fut.getUninterruptedly();

            fail("Error was expected.");
        } catch (Exception err) {
            assertFalse(fut.isSuccess());

            assertEquals(errorType(), err.getClass());
            assertSame(TEST_ERROR, err.getCause());
        }
    }
}
