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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Meta-information about RPC method.
 *
 * @see RpcInterfaceInfo#methods()
 */
public class RpcMethodInfo {
    private final String signature;

    private final boolean async;

    private final Optional<RpcAggregate> aggregate;

    private final Optional<RpcBroadcast> broadcast;

    private final OptionalInt splitArg;

    private final OptionalInt affinityArg;

    private final Optional<RpcRetryInfo> retry;

    @ToStringIgnore
    private final Class<?> realReturnType;

    @ToStringIgnore
    private final Method javaMethod;

    /**
     * Constructs a new instance.
     *
     * @param javaMethod Java method.
     */
    public RpcMethodInfo(Method javaMethod) {
        ArgAssert.notNull(javaMethod, "Java method");

        this.signature = shortSignature(javaMethod);
        this.javaMethod = javaMethod;
        this.affinityArg = findAffinityArg(javaMethod);
        this.splitArg = findSplitArg(javaMethod);
        this.async = isAsyncReturnType(javaMethod);
        this.aggregate = findAggregate(javaMethod);
        this.broadcast = findBroadcast(javaMethod);
        this.retry = findRetry(javaMethod);

        if (broadcast.isPresent()) {
            this.realReturnType = Void.class;
        } else {
            this.realReturnType = findRealReturnType(javaMethod);
        }

        if (broadcast.isPresent() && aggregate.isPresent()) {
            throw new IllegalArgumentException("@" + RpcAggregate.class.getSimpleName() + " can't be used together with "
                + "@" + RpcBroadcast.class.getSimpleName() + " [method=" + javaMethod + ']');
        }

        if (splitArg.isPresent()) {
            if (affinityArg.isPresent()) {
                throw new IllegalArgumentException("@" + RpcSplit.class.getSimpleName() + " can't be used together with "
                    + "@" + RpcAffinityKey.class.getSimpleName() + " [method=" + javaMethod + ']');
            }

            if (!aggregate.isPresent()) {
                throw new IllegalArgumentException("@" + RpcSplit.class.getSimpleName() + " can be used only in "
                    + "@" + RpcAggregate.class.getSimpleName() + "-annotated methods [method=" + javaMethod + ']');
            }
        }
    }

    /**
     * Returns the signature of this method.
     *
     * @return Signature of this method.
     */
    public String signature() {
        return signature;
    }

    /**
     * Returns the Java method.
     *
     * @return Java method.
     */
    public Method javaMethod() {
        return javaMethod;
    }

    /**
     * Returns {@code true} if this is an asynchronous method.
     *
     * @return {@code true} if this is an asynchronous method.
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * Returns the {@link RpcAggregate} annotation that is declared on this method (if presents).
     *
     * @return {@link RpcAggregate} annotation.
     */
    public Optional<RpcAggregate> aggregate() {
        return aggregate;
    }

    /**
     * Returns the {@link RpcBroadcast} annotation that is declared on this method (if presents).
     *
     * @return {@link RpcBroadcast} annotation.
     */
    public Optional<RpcBroadcast> broadcast() {
        return broadcast;
    }

    /**
     * Returns the zero-based index of an argument that is annotated with {@link RpcSplit} (if presents).
     *
     * @return Zero-based index of an argument that is annotated with {@link RpcSplit}.
     */
    public OptionalInt splitArg() {
        return splitArg;
    }

    /**
     * Returns the type of a {@link #splitArg()}.
     *
     * @return Type of a {@link #splitArg()}.
     */
    public Optional<Class<?>> splitArgType() {
        if (splitArg.isPresent()) {
            return Optional.of(javaMethod.getParameterTypes()[splitArg.getAsInt()]);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the real return type of this method.
     *
     * <p>
     * Real return type is a type that is produced by this method in a form of a direct result or in a form of a deferred result (via
     * parametrization of the {@link CompletableFuture} return type).
     * </p>
     *
     * @return Real return type of this method.
     */
    public Class<?> realReturnType() {
        return realReturnType;
    }

    /**
     * Returns the error retry policy of this method based on the {@link RpcRetry} annotation attributes.
     *
     * @return Error retry policy.
     *
     * @see RpcRetry
     */
    public Optional<RpcRetryInfo> retry() {
        return retry;
    }

    /**
     * Returns the zero-based index of an argument that is annotated with {@link RpcAffinityKey} (if presents).
     *
     * @return Zero-based index of an argument that is annotated with {@link RpcAffinityKey}.
     */
    public OptionalInt affinityArg() {
        return affinityArg;
    }

    private static String shortSignature(Method method) {
        StringBuilder buf = new StringBuilder().append(method.getName()).append('(');

        for (Class<?> param : method.getParameterTypes()) {
            if (buf.charAt(buf.length() - 1) != '(') {
                buf.append(',');
            }

            buf.append(param.getCanonicalName());
        }

        buf.append(')');

        return buf.toString();
    }

    private static boolean isAsyncReturnType(Method meth) {
        return CompletableFuture.class.equals(meth.getReturnType());
    }

    private static Optional<RpcAggregate> findAggregate(Method meth) {
        Optional<RpcAggregate> aggregate = Optional.ofNullable(meth.getAnnotation(RpcAggregate.class));

        if (aggregate.isPresent()) {
            Class<?> returnType = findRealReturnType(meth);

            if (!Collection.class.equals(returnType)
                && !Set.class.equals(returnType)
                && !List.class.equals(returnType)
                && !Map.class.equals(returnType)) {
                // Allowed types info for the error message.
                String col = Collection.class.getSimpleName();
                String lst = List.class.getSimpleName();
                String set = Set.class.getSimpleName();
                String map = Map.class.getSimpleName();

                String future = CompletableFuture.class.getSimpleName() + "<" + col + "|" + lst + "|" + set + "|" + map + ">";

                throw new IllegalArgumentException("Method annotated with @" + RpcAggregate.class.getSimpleName() + " has unsupported "
                    + "return type [supported-types={" + col + ", " + lst + ", " + set + ", " + map + ", " + future + "}, "
                    + "method=" + meth + ']');
            }
        }

        return aggregate;
    }

    private static Optional<RpcBroadcast> findBroadcast(Method meth) {
        return Optional.ofNullable(meth.getAnnotation(RpcBroadcast.class));
    }

    private static Optional<RpcRetryInfo> findRetry(Method meth) {
        return Optional.ofNullable(meth.getAnnotation(RpcRetry.class)).map(RpcRetryInfo::parse);
    }

    private static OptionalInt findSplitArg(Method meth) {
        OptionalInt splitIdx = OptionalInt.empty();

        Annotation[][] parameters = meth.getParameterAnnotations();

        for (int i = 0; i < parameters.length; i++) {
            Annotation[] annotations = parameters[i];

            for (int j = 0; j < annotations.length; j++) {
                if (annotations[j].annotationType().equals(RpcSplit.class)) {
                    if (splitIdx.isPresent()) {
                        throw new IllegalArgumentException("Only one argument can be annotated with @" + RpcSplit.class.getSimpleName()
                            + " [method=" + meth + ']');
                    }

                    splitIdx = OptionalInt.of(i);
                }
            }
        }

        if (splitIdx.isPresent()) {
            Class<?> splitType = meth.getParameterTypes()[splitIdx.getAsInt()];

            // Verify parameter type.
            if (!Collection.class.equals(splitType)
                && !Set.class.equals(splitType)
                && !List.class.equals(splitType)
                && !Map.class.equals(splitType)) {
                // Allowed types info for the error message.
                String col = Collection.class.getSimpleName();
                String lst = List.class.getSimpleName();
                String set = Set.class.getSimpleName();
                String map = Map.class.getSimpleName();

                throw new IllegalArgumentException("Parameter annotated with @" + RpcSplit.class.getSimpleName() + " has unsupported "
                    + "type [supported-types={" + col + ", " + lst + ", " + set + ", " + map + "}, method=" + meth + ']');
            }
        }

        return splitIdx;
    }

    private static OptionalInt findAffinityArg(Method meth) {
        OptionalInt affinityArg = OptionalInt.empty();

        if (meth.getParameterCount() > 0) {
            for (int i = 0; i < meth.getParameterCount() && !affinityArg.isPresent(); i++) {
                Annotation[] annotations = meth.getParameterAnnotations()[i];

                for (int j = 0; j < annotations.length; j++) {
                    if (RpcAffinityKey.class.isAssignableFrom(annotations[j].annotationType())) {
                        affinityArg = OptionalInt.of(i);

                        break;
                    }
                }
            }
        }

        return affinityArg;
    }

    private static Class<?> findRealReturnType(Method meth) {
        Class<?> returnType = meth.getReturnType();

        if (CompletableFuture.class.equals(returnType)) {
            Type[] typeArgs = ((ParameterizedType)meth.getGenericReturnType()).getActualTypeArguments();

            if (typeArgs.length == 1) {
                Type typeArg = typeArgs[0];

                if (typeArg instanceof Class) {
                    returnType = (Class<?>)typeArg;
                } else if (typeArg instanceof ParameterizedType) {
                    ParameterizedType paramTypeArg = (ParameterizedType)typeArg;

                    returnType = (Class<?>)paramTypeArg.getRawType();
                }
            }
        }

        return returnType;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
