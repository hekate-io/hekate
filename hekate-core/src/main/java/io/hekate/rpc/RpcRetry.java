/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.rpc;

import io.hekate.messaging.retry.ExponentialBackoffPolicy;
import io.hekate.messaging.retry.FixedBackoffPolicy;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryPolicy;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation to enable retrying of failed RPC invocations.
 *
 * <p>
 * This annotation can be placed on a method of an {@link Rpc}-annotated interface in order to enable retrying of failed invocations.
 * </p>
 *
 * <p>
 * Generic parameters of the retry logic (like maximum attempts, delay between attempts, etc) can be specified at the RPC-client level
 * via the {@link RpcClientConfig#setRetryPolicy(GenericRetryConfigurer)} or via the
 * {@link RpcClientBuilder#withRetryPolicy(GenericRetryConfigurer)} method.
 * Specifying those parameters in the annotation attributes overrides those values (i.e. annotation attributes has higher priority over
 * the generic defaults).
 * </p>
 *
 * @see RpcService
 */
@Inherited
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RpcRetry {
    /**
     * Base types of errors that can be retried. If not specified then all errors will be retried.
     *
     * @return Base types of errors that can be retried.
     *
     * @see RetryPolicy#whileError(RetryErrorPredicate)
     */
    Class<? extends Throwable>[] errors() default {};

    /**
     * Maximum amount of attempts. Zero to disable retries, negative value for unlimited attempts.
     *
     * @return Maximum amount of attempts.
     *
     * @see RetryPolicy#maxAttempts(int)
     */
    String maxAttempts() default "";

    /**
     * Delay in milliseconds between attempts.
     *
     * <p>
     * If this attribute is set but the {@link #maxDelay()} is not set, then the {@link FixedBackoffPolicy} will be used.
     * If {@link #maxDelay()} attribute is also set then the {@link ExponentialBackoffPolicy} will be used.
     * </p>
     *
     * @return Delay in milliseconds between attempts.
     *
     * @see RetryPolicy#withFixedDelay(long)
     */
    String delay() default "";

    /**
     * Maximum delay in milliseconds between attempts.
     *
     * <p>
     * If this attribute is set then the {@link ExponentialBackoffPolicy} will be used to calculate delay between attempts. In  such case
     * the {@link #delay()} attribute's value will be used as a base delay and the value of this attribute as a maximum delay.
     * </p>
     *
     * @return Maximum delay in milliseconds between attempts.
     *
     * @see RetryPolicy#withExponentialDelay(long, long)
     */
    String maxDelay() default "";
}
