/*
 * Copyright 2017 The Hekate Project
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

    private final OptionalInt affinityArg;

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
        this.async = isAsyncReturnType(javaMethod);
        this.realReturnType = findRealReturnType(javaMethod);
        this.aggregate = findAggregate(javaMethod);
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
     * Returns the zero-based index of an argument that is annotated with {@link RpcAffinityKey} (if presents).
     *
     * @return Zero-based index of an argument that is annotated with {@link RpcAffinityKey}.
     */
    public OptionalInt affinityArg() {
        return affinityArg;
    }

    private static String shortSignature(Method method) {
        StringBuilder buf = new StringBuilder();

        buf.append(method.getName());
        buf.append('(');

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

        Class<?> returnType = findRealReturnType(meth);

        if (aggregate.isPresent()
            && !Collection.class.equals(returnType)
            && !Set.class.equals(returnType)
            && !List.class.equals(returnType)
            && !Map.class.equals(returnType)) {
            String col = Collection.class.getSimpleName();
            String lst = List.class.getSimpleName();
            String set = Set.class.getSimpleName();
            String map = Map.class.getSimpleName();

            String future = CompletableFuture.class.getSimpleName() + "<" + col + "|" + lst + "|" + set + "|" + map + ">";

            throw new IllegalArgumentException("Method annotated with @" + RpcAggregate.class.getSimpleName() + " has unsupported "
                + "return type [supported-types={" + col + ", " + lst + ", " + set + ", " + map + ", " + future + "}, "
                + "method=" + meth + ']');
        }

        return aggregate;
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
