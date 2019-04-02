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

package io.hekate.rpc.internal;

import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;

class RpcTypeAnalyzer {
    private final ConcurrentMap<Class<?>, List<RpcInterfaceInfo<?>>> cache = new ConcurrentHashMap<>();

    public <T> RpcInterfaceInfo<T> analyzeType(Class<T> type) {
        @SuppressWarnings("unchecked")
        RpcInterfaceInfo<T> info = (RpcInterfaceInfo<T>)cached(type).stream()
            .filter(t -> t.javaType() == type)
            .findFirst()
            .orElseThrow(() ->
                new IllegalArgumentException(type.getName() + " must be a public interface "
                    + "and must be annotated with @" + Rpc.class.getSimpleName() + '.')
            );

        return info;
    }

    public List<RpcInterface<?>> analyze(Object target) {
        Class<?> type = target.getClass();

        List<RpcInterfaceInfo<?>> rpcInterfaces = cached(type);

        return rpcInterfaces.stream()
            .map(info -> new RpcInterface<>(info, target))
            .collect(Collectors.toList());
    }

    private List<RpcInterfaceInfo<?>> cached(Class<?> type) {
        return cache.computeIfAbsent(type, RpcTypeAnalyzer::doAnalyzeType);
    }

    private static List<RpcInterfaceInfo<?>> doAnalyzeType(Class<?> type) {
        List<Class<?>> allInterfaces = findAllInterfaces(type);

        List<RpcInterfaceInfo<?>> collected = new ArrayList<>();

        doAnalyzeType(allInterfaces, collected, new HashSet<>());

        return collected;
    }

    private static void doAnalyzeType(List<Class<?>> types, List<RpcInterfaceInfo<?>> rpcInterfaces, Set<Class<?>> uniqueTypes) {
        for (Class<?> javaType : types) {
            if (javaType.isInterface() && isPublic(javaType.getModifiers())) {
                if (uniqueTypes == null || !uniqueTypes.contains(javaType)) {
                    Rpc rpc = javaType.getAnnotation(Rpc.class);

                    if (rpc != null) {
                        if (rpc.version() < rpc.minClientVersion()) {
                            throw new IllegalArgumentException('@' + Rpc.class.getSimpleName() + " version must be greater than or equals "
                                + "to the minimum client version [type=" + javaType.getName() + ", version=" + rpc.version()
                                + ", min-client-version=" + rpc.minClientVersion() + ']');
                        }

                        Method[] methods = javaType.getMethods();

                        List<RpcMethodInfo> methodsInfo = new ArrayList<>(methods.length);

                        for (Method meth : methods) {
                            if (isPublic(meth.getModifiers()) && !isStatic(meth.getModifiers())) {
                                methodsInfo.add(new RpcMethodInfo(meth));
                            }
                        }

                        if (!methodsInfo.isEmpty()) {
                            rpcInterfaces.add(new RpcInterfaceInfo<>(javaType, rpc.version(), rpc.minClientVersion(), methodsInfo));
                        }
                    }

                    if (uniqueTypes != null) {
                        uniqueTypes.add(javaType);

                        doAnalyzeType(asList(javaType.getInterfaces()), rpcInterfaces, uniqueTypes);
                    }
                }
            }
        }
    }

    private static List<Class<?>> findAllInterfaces(Class<?> type) {
        List<Class<?>> interfaces = new ArrayList<>();

        Stream.of(type.getInterfaces())
            .filter(face -> isPublic(face.getModifiers()))
            .forEach(interfaces::add);

        if (type.isInterface() && isPublic(type.getModifiers())) {
            interfaces.add(type);
        }

        return interfaces;
    }
}
