package io.hekate.rpc.internal;

import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;

final class RpcTypeAnalyzer {
    private RpcTypeAnalyzer() {
        // No-op.
    }

    public static <T> RpcInterfaceInfo<T> analyzeType(Class<T> type) {
        @SuppressWarnings("unchecked")
        RpcInterfaceInfo<T> info = (RpcInterfaceInfo<T>)doAnalyzeType(type).stream()
            .filter(t -> t.javaType() == type)
            .findFirst()
            .orElseThrow(() ->
                new IllegalArgumentException(type.getName() + " must be a public interface "
                    + "and must be annotated with @" + Rpc.class.getSimpleName() + '.')
            );

        return info;
    }

    public static List<RpcInterface<?>> analyze(Object target) {
        List<RpcInterfaceInfo<?>> rpcInterfaces = doAnalyzeType(target.getClass());

        return rpcInterfaces.stream()
            .map(info -> new RpcInterface<>(info, target))
            .collect(Collectors.toList());
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
