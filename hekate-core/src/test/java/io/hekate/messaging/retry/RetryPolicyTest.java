package io.hekate.messaging.retry;

import io.hekate.HekateTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryPolicyTest extends HekateTestBase {
    private RetryPolicy<?> policy;

    @Before
    public void setUp() throws Exception {
        policy = mock(RetryPolicy.class);
    }

    @Test
    public void testFixedDelay() {
        when(policy.withFixedDelay(anyLong())).thenCallRealMethod();

        policy.withFixedDelay(100500);

        verify(policy).withBackoff(argThat(arg -> arg.delayBeforeRetry(1) == 100500));
    }

    @Test
    public void testExponentialDelay() {
        when(policy.withExponentialDelay(anyLong(), anyLong())).thenCallRealMethod();

        policy.withExponentialDelay(100500, 500100);

        verify(policy).withBackoff(argThat(arg -> {
            ExponentialBackoffPolicy exp = (ExponentialBackoffPolicy)arg;

            return exp.baseDelay() == 100500 && exp.maxDelay() == 500100;
        }));
    }

    @Test
    public void testUnlimitedAttempts() {
        when(policy.unlimitedAttempts()).thenCallRealMethod();

        policy.unlimitedAttempts();

        verify(policy).maxAttempts(eq(-1));
    }
}
