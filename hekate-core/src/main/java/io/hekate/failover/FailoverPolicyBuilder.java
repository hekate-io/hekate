/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.failover;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Builder for {@link FailoverPolicy}.
 */
public class FailoverPolicyBuilder {
    private FailoverDelaySupplier retryDelay;

    private List<FailoverCondition> retryUntil;

    private List<Class<? extends Throwable>> errorTypes;

    private FailoverRoutingPolicy routingPolicy;

    /**
     * Builds a new policy by copying all configuration options from this builder.
     *
     * @return New failover policy.
     */
    public FailoverPolicy build() {
        if (retryUntil == null && retryDelay == null) {
            return FailoverPolicy.alwaysFail();
        }

        List<FailoverCondition> localRetryUntil = StreamUtils.nullSafe(retryUntil).collect(toList());

        if (localRetryUntil.isEmpty()) {
            return FailoverPolicy.alwaysFail();
        }

        List<Class<? extends Throwable>> localErrorTypes = StreamUtils.nullSafe(errorTypes).collect(toList());

        return doBuild(localRetryUntil, retryDelay, localErrorTypes, routingPolicy);
    }

    /**
     * Returns the list of error types that can be supported by a failover policy (see {@link #setErrorTypes(List)}).
     *
     * @return LIst of error types.
     */
    public List<Class<? extends Throwable>> getErrorTypes() {
        return errorTypes;
    }

    /**
     * Sets the list of error types that can be supported by a failover policy.
     *
     * <p>
     * If none of the specified types matches with a {@link FailureInfo#error() failure} then failover policy is to {@link
     * FailoverContext#fail() fail}.
     * </p>
     *
     * <p>
     * If error types are not specified then failover policy will be applied to all error types.
     * </p>
     *
     * @param errorTypes Error types.
     */
    public void setErrorTypes(List<Class<? extends Throwable>> errorTypes) {
        this.errorTypes = errorTypes;
    }

    /**
     * Fluent-style version of {@link #setErrorTypes(List)}.
     *
     * @param errorType Error type.
     *
     * @return This instance.
     */
    public FailoverPolicyBuilder withErrorTypes(Class<? extends Throwable> errorType) {
        ArgAssert.notNull(errorType, "Error type");

        if (errorTypes == null) {
            errorTypes = new ArrayList<>();
        }

        errorTypes.add(errorType);

        return this;
    }

    /**
     * Fluent-style version of {@link #setErrorTypes(List)}.
     *
     * @param errorTypes Error types.
     *
     * @return This instance.
     */
    @SafeVarargs
    public final FailoverPolicyBuilder withErrorTypes(Class<? extends Throwable>... errorTypes) {
        ArgAssert.notNull(errorTypes, "Error types array");

        for (Class<? extends Throwable> errorType : errorTypes) {
            withErrorTypes(errorType);
        }

        return this;
    }

    /**
     * Returns the {@link FailureResolution#delay() retry delay} supplier (see {@link #setRetryDelay(FailoverDelaySupplier)}).
     *
     * @return Supplier of {@link FailureResolution#delay() retry delay} value.
     */
    public FailoverDelaySupplier getRetryDelay() {
        return retryDelay;
    }

    /**
     * Sets the {@link FailureResolution#delay() retry delay} supplier. This supplier will be used by a failover policy to get a retry
     * delay value for the {@link FailureResolution#withDelay(long)} method.
     *
     * @param retryDelay Delay supplier.
     */
    public void setRetryDelay(FailoverDelaySupplier retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Fluent-style version of {@link #setRetryDelay(FailoverDelaySupplier)}.
     *
     * @param retryDelay Delay supplier.
     *
     * @return This instance.
     */
    public FailoverPolicyBuilder withRetryDelay(FailoverDelaySupplier retryDelay) {
        setRetryDelay(retryDelay);

        return this;
    }

    /**
     * Shortcut method to apply {@link ConstantFailoverDelay} to this builder.
     *
     * @param delay Delay in milliseconds (see {@link ConstantFailoverDelay#ConstantFailoverDelay(long)}).
     *
     * @return This instance.
     */
    public FailoverPolicyBuilder withConstantRetryDelay(long delay) {
        return withRetryDelay(new ConstantFailoverDelay(delay));
    }

    /**
     * Returns the list of failover conditions (see {@link #setRetryUntil(List)}).
     *
     * @return List of failover conditions.
     */
    public List<FailoverCondition> getRetryUntil() {
        return retryUntil;
    }

    /**
     * Sets the list of failover conditions that should be checked by a failover policy before trying to apply failover actions.
     *
     * <p>
     * All conditions are checked one by one and if at least one of them returns {@code false} then failover actions will not be applied
     * and operation will {@link FailoverContext#fail() fail}.
     * </p>
     *
     * <p>
     * If failover conditions are not specified then failover policy will always {@link FailoverContext#fail() fail}.
     * </p>
     *
     * @param retryUntil Failover conditions.
     */
    public void setRetryUntil(List<FailoverCondition> retryUntil) {
        this.retryUntil = retryUntil;
    }

    /**
     * Fluent style version of {@link #setRetryUntil(List)}.
     *
     * @param retryUntil Failover conditions.
     *
     * @return This instance.
     */
    public FailoverPolicyBuilder withRetryUntil(FailoverCondition retryUntil) {
        ArgAssert.notNull(retryUntil, "Failover condition");

        if (this.retryUntil == null) {
            this.retryUntil = new ArrayList<>();
        }

        this.retryUntil.add(retryUntil);

        return this;
    }

    /**
     * Shortcut method to {@link #setRetryUntil(List)} register} a {@link MaxFailoverAttempts} condition to this builder.
     *
     * @param attempts Maximum number of attempts (see {@link MaxFailoverAttempts#MaxFailoverAttempts(int)}).
     *
     * @return This instance.
     */
    public FailoverPolicyBuilder withMaxAttempts(int attempts) {
        return withRetryUntil(new MaxFailoverAttempts(attempts));
    }

    /**
     * Returns the routing policy that should be used during failover actions (see {@link #setRoutingPolicy(FailoverRoutingPolicy)}).
     *
     * @return Routing policy.
     */
    public FailoverRoutingPolicy getRoutingPolicy() {
        return routingPolicy;
    }

    /**
     * Sets the routing policy that should be used during failover actions.
     *
     * @param routingPolicy Routing policy.
     */
    public void setRoutingPolicy(FailoverRoutingPolicy routingPolicy) {
        this.routingPolicy = routingPolicy;
    }

    /**
     * Applies {@link FailoverRoutingPolicy#RE_ROUTE} policy to this builder.
     *
     * @return This instance.
     *
     * @see #setRoutingPolicy(FailoverRoutingPolicy)
     */
    public FailoverPolicyBuilder withAlwaysReRoute() {
        routingPolicy = FailoverRoutingPolicy.RE_ROUTE;

        return this;
    }

    /**
     * Applies {@link FailoverRoutingPolicy#RETRY_SAME_NODE} policy to this builder.
     *
     * @return This instance.
     *
     * @see #setRoutingPolicy(FailoverRoutingPolicy)
     */
    public FailoverPolicyBuilder withAlwaysRetrySameNode() {
        routingPolicy = FailoverRoutingPolicy.RETRY_SAME_NODE;

        return this;
    }

    /**
     * Applies {@link FailoverRoutingPolicy#PREFER_SAME_NODE} policy to this builder.
     *
     * @return This instance.
     *
     * @see #setRoutingPolicy(FailoverRoutingPolicy)
     */
    public FailoverPolicyBuilder withRetrySameNodeIfExists() {
        routingPolicy = FailoverRoutingPolicy.PREFER_SAME_NODE;

        return this;
    }

    private static FailoverPolicy doBuild(List<FailoverCondition> retryUntil, FailoverDelaySupplier retryDelay,
        List<Class<? extends Throwable>> errTypes, FailoverRoutingPolicy routingPolicy) {
        return new FailoverPolicy() {
            @Override
            public FailureResolution apply(FailoverContext ctx) {
                if (errTypes != null && !errTypes.isEmpty()) {
                    boolean found = false;

                    for (Class<? extends Throwable> errorType : errTypes) {
                        if (ctx.isCausedBy(errorType)) {
                            found = true;

                            break;
                        }
                    }

                    if (!found) {
                        return ctx.fail();
                    }
                }

                for (FailoverCondition condition : retryUntil) {
                    if (!condition.test(ctx)) {
                        return ctx.fail();
                    }
                }

                FailureResolution retry = ctx.retry();

                if (retryDelay != null) {
                    retry.withDelay(retryDelay.delayOf(ctx));
                }

                if (routingPolicy != null) {
                    retry.withRoutingPolicy(routingPolicy);
                }

                return retry;
            }

            @Override
            public String toString() {
                return FailoverPolicy.class.getSimpleName()
                    + "[error-types=" + errTypes
                    + ", retry-delay=" + retryDelay
                    + ", retry-until=" + retryUntil
                    + ']';
            }
        };
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
