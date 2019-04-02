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

import io.hekate.HekateTestBase;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcAffinityKey;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcBroadcast;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcSplit;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RpcTypeAnalyzerTest extends HekateTestBase {
    @Rpc
    @SuppressWarnings("unused")
    interface NonPublic {
        void nonPublic();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface TypeA {
        void methA1();
    }

    @Rpc(version = 1)
    @SuppressWarnings("unused")
    public interface TypeB {
        void methB1();

        void methB2();
    }

    @Rpc(version = 2, minClientVersion = 1)
    @SuppressWarnings("unused")
    public interface TypeC extends TypeA, TypeB {
        void methC1();

        void methC2();
    }

    @SuppressWarnings("unused")
    public interface NotAnRpc {
        void someMethod();
    }

    @Rpc(version = 1, minClientVersion = 3)
    @SuppressWarnings("unused")
    public interface InvalidMinClientVersion {
        void someMethod();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcWithAffinity {
        void key1(@RpcAffinityKey Object arg);

        void key2(Object arg1, @RpcAffinityKey Object arg2);

        void key3(Object arg1, @RpcAffinityKey Object arg2, Object arg3);
    }

    @Rpc
    public interface AggregateType {
        @RpcAggregate
        Collection<Object> collection();

        @RpcAggregate
        List<Object> list();

        @RpcAggregate
        Set<Object> set();

        @RpcAggregate
        Map<Object, Object> map();
    }

    @Rpc
    public interface AggregateAsyncType {
        @RpcAggregate
        CompletableFuture<Collection<Object>> collection();

        @RpcAggregate
        CompletableFuture<List<Object>> list();

        @RpcAggregate
        CompletableFuture<Set<Object>> set();

        @RpcAggregate
        CompletableFuture<Map<Object, Object>> map();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface BroadcastType {
        @RpcBroadcast
        void call();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface BroadcastAsyncType {
        @RpcBroadcast
        CompletableFuture<?> call();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface InvalidAggregateTypeA {
        @RpcAggregate
        void someMethod();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface InvalidAggregateTypeB {
        @RpcAggregate
        ArrayList<?> someMethod();
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcWithDuplicatedAffinity {
        void key(@RpcAffinityKey Object arg1, @RpcAffinityKey Object arg2);
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcWithDuplicatedSplit {
        @RpcAggregate
        List<?> method(@RpcSplit List<Object> arg1, @RpcSplit List<Object> arg2);
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcWithInvalidSplitType {
        @RpcAggregate
        List<?> method(@RpcSplit Object arg);
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcWithAffinityAndSplit {
        @RpcAggregate
        List<?> method(@RpcAffinityKey Object key, @RpcSplit List<Object> arg);
    }

    @Rpc
    @SuppressWarnings("unused")
    public interface RpcSplitWithoutAggregate {
        List<?> method(@RpcSplit List<Object> arg);
    }

    private final RpcTypeAnalyzer analyzer = new RpcTypeAnalyzer();

    @Test
    public void testAnalyzeWithoutRpcAnnotation() {
        assertTrue(analyzer.analyze(mock(NotAnRpc.class)).isEmpty());
    }

    @Test
    public void testAnalyzeNonPublic() {
        assertTrue(analyzer.analyze(mock(NonPublic.class)).isEmpty());
    }

    @Test
    public void testAnalyzeA() throws Exception {
        List<RpcInterface<?>> rpcs = analyzer.analyze(mock(TypeA.class));

        assertEquals(1, rpcs.size());

        @SuppressWarnings("unchecked")
        RpcInterface<TypeA> rpc = (RpcInterface<TypeA>)rpcs.get(0);

        verifyRpcA(rpc);
    }

    @Test
    public void testAnalyzeB() throws Exception {
        List<RpcInterface<?>> rpcs = analyzer.analyze(mock(TypeB.class));

        assertEquals(1, rpcs.size());

        @SuppressWarnings("unchecked")
        RpcInterface<TypeB> rpc = (RpcInterface<TypeB>)rpcs.get(0);

        verifyRpcB(rpc);
    }

    @Test
    public void testAnalyzeC() throws Exception {
        List<RpcInterface<?>> rpcs = analyzer.analyze(mock(TypeC.class));

        assertEquals(3, rpcs.size());

        @SuppressWarnings("unchecked")
        RpcInterface<TypeA> rpcA = (RpcInterface<TypeA>)rpcs.stream()
            .filter(f -> f.type().javaType() == TypeA.class)
            .findFirst()
            .orElseThrow(AssertionError::new);

        @SuppressWarnings("unchecked")
        RpcInterface<TypeB> rpcB = (RpcInterface<TypeB>)rpcs.stream()
            .filter(f -> f.type().javaType() == TypeB.class)
            .findFirst()
            .orElseThrow(AssertionError::new);

        @SuppressWarnings("unchecked")
        RpcInterface<TypeC> rpcC = (RpcInterface<TypeC>)rpcs.stream()
            .filter(f -> f.type().javaType() == TypeC.class)
            .findFirst()
            .orElseThrow(AssertionError::new);

        verifyRpcA(rpcA);
        verifyRpcB(rpcB);

        RpcInterfaceInfo<TypeC> typeC = rpcC.type();

        assertSame(TypeC.class, typeC.javaType());
        assertEquals(2, typeC.version());
        assertEquals(1, typeC.minClientVersion());
        assertEquals(TypeC.class.getName(), typeC.name());
        assertEquals(TypeC.class.getName() + ":2", typeC.versionedName());

        assertEquals(5, typeC.methods().size());
        assertTrue(typeC.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methA1")));
        assertTrue(typeC.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methB1")));
        assertTrue(typeC.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methB2")));
        assertTrue(typeC.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methC1")));
        assertTrue(typeC.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methC2")));
    }

    @Test
    public void testAnalyzeType() {
        expect(IllegalArgumentException.class, () -> analyzer.analyzeType(Runnable.class));

        RpcInterfaceInfo<TypeA> info = analyzer.analyzeType(TypeA.class);

        verifyTypeA(info);
    }

    @Test
    public void testAnalyzeTypeCache() {
        RpcInterfaceInfo<TypeA> info1 = analyzer.analyzeType(TypeA.class);
        RpcInterfaceInfo<TypeA> info2 = analyzer.analyzeType(TypeA.class);
        RpcInterfaceInfo<TypeA> info3 = analyzer.analyzeType(TypeA.class);

        verifyTypeA(info1);

        assertSame(info1, info2);
        assertSame(info1, info3);
    }

    @Test
    public void testAffinityKey() {
        RpcInterfaceInfo<RpcWithAffinity> type = analyzer.analyzeType(RpcWithAffinity.class);

        assertEquals(3, type.methods().size());

        RpcMethodInfo m1 = findMethod("key1", type);
        RpcMethodInfo m2 = findMethod("key2", type);
        RpcMethodInfo m3 = findMethod("key3", type);

        assertEquals(m1.toString(), 0, m1.affinityArg().getAsInt());
        assertEquals(m2.toString(), 1, m2.affinityArg().getAsInt());
        assertEquals(m3.toString(), 1, m3.affinityArg().getAsInt());
    }

    @Test
    public void testDuplicatedAffinityKey() {
        RpcInterfaceInfo<RpcWithDuplicatedAffinity> type = analyzer.analyzeType(RpcWithDuplicatedAffinity.class);

        assertEquals(1, type.methods().size());

        RpcMethodInfo m1 = type.methods().get(0);

        assertEquals(m1.toString(), 0, m1.affinityArg().getAsInt());
    }

    @Test
    public void testInvalidMinClientVersion() {
        expectExactMessage(IllegalArgumentException.class,
            '@' + Rpc.class.getSimpleName() + " version must be greater than or equals to the minimum client version "
                + "[type=" + InvalidMinClientVersion.class.getName() + ", version=1, min-client-version=3]",
            () -> analyzer.analyzeType(InvalidMinClientVersion.class));
    }

    @Test
    public void testBroadcastType() {
        RpcInterfaceInfo<BroadcastType> type = analyzer.analyzeType(BroadcastType.class);

        RpcMethodInfo call = findMethod("call", type);

        assertTrue(call.broadcast().isPresent());

        assertSame(Void.class, call.realReturnType());
    }

    @Test
    public void testBroadcastAsyncType() {
        RpcInterfaceInfo<BroadcastAsyncType> type = analyzer.analyzeType(BroadcastAsyncType.class);

        RpcMethodInfo call = findMethod("call", type);

        assertTrue(call.broadcast().isPresent());

        assertTrue(call.isAsync());

        assertSame(Void.class, call.realReturnType());
    }

    @Test
    public void testAggregateType() {
        RpcInterfaceInfo<AggregateType> type = analyzer.analyzeType(AggregateType.class);

        RpcMethodInfo col = findMethod("collection", type);
        RpcMethodInfo lst = findMethod("list", type);
        RpcMethodInfo set = findMethod("set", type);
        RpcMethodInfo map = findMethod("map", type);

        assertTrue(col.aggregate().isPresent());
        assertTrue(lst.aggregate().isPresent());
        assertTrue(set.aggregate().isPresent());
        assertTrue(map.aggregate().isPresent());

        assertFalse(col.isAsync());
        assertFalse(lst.isAsync());
        assertFalse(set.isAsync());
        assertFalse(map.isAsync());

        assertSame(Collection.class, col.realReturnType());
        assertSame(List.class, lst.realReturnType());
        assertSame(Set.class, set.realReturnType());
        assertSame(Map.class, map.realReturnType());
    }

    @Test
    public void testAggregateAsyncType() {
        RpcInterfaceInfo<AggregateAsyncType> type = analyzer.analyzeType(AggregateAsyncType.class);

        RpcMethodInfo col = findMethod("collection", type);
        RpcMethodInfo lst = findMethod("list", type);
        RpcMethodInfo set = findMethod("set", type);
        RpcMethodInfo map = findMethod("map", type);

        assertTrue(col.aggregate().isPresent());
        assertTrue(lst.aggregate().isPresent());
        assertTrue(set.aggregate().isPresent());
        assertTrue(map.aggregate().isPresent());

        assertTrue(col.isAsync());
        assertTrue(lst.isAsync());
        assertTrue(set.isAsync());
        assertTrue(map.isAsync());

        assertSame(Collection.class, col.realReturnType());
        assertSame(List.class, lst.realReturnType());
        assertSame(Set.class, set.realReturnType());
        assertSame(Map.class, map.realReturnType());
    }

    @Test
    public void testAggregateReturnVoidType() {
        expectExactMessage(IllegalArgumentException.class,
            "Method annotated with @RpcAggregate has unsupported return type ["
                + "supported-types={Collection, List, Set, Map, CompletableFuture<Collection|List|Set|Map>}, "
                + "method=public abstract void " + InvalidAggregateTypeA.class.getName() + ".someMethod()]",
            () -> analyzer.analyzeType(InvalidAggregateTypeA.class));
    }

    @Test
    public void testAggregateReturnConcreteType() {
        expectExactMessage(IllegalArgumentException.class,
            "Method annotated with @RpcAggregate has unsupported return type ["
                + "supported-types={Collection, List, Set, Map, CompletableFuture<Collection|List|Set|Map>}, "
                + "method=public abstract java.util.ArrayList " + InvalidAggregateTypeB.class.getName() + ".someMethod()]",
            () -> analyzer.analyzeType(InvalidAggregateTypeB.class));
    }

    @Test
    public void testMultipleSplit() {
        expectExactMessage(IllegalArgumentException.class,
            "Only one argument can be annotated with @RpcSplit [method="
                + "public abstract java.util.List " + RpcWithDuplicatedSplit.class.getName() + ".method(java.util.List,java.util.List)]",
            () -> analyzer.analyzeType(RpcWithDuplicatedSplit.class));
    }

    @Test
    public void testInvalidSplitType() {
        expectExactMessage(IllegalArgumentException.class,
            "Parameter annotated with @RpcSplit has unsupported type ["
                + "supported-types={Collection, List, Set, Map}, "
                + "method=public abstract java.util.List " + RpcWithInvalidSplitType.class.getName() + ".method(java.lang.Object)]",
            () -> analyzer.analyzeType(RpcWithInvalidSplitType.class));
    }

    @Test
    public void testSplitWithAffinity() {
        expectExactMessage(IllegalArgumentException.class,
            "@RpcSplit can't be used together with @RpcAffinityKey [method="
                + "public abstract java.util.List " + RpcWithAffinityAndSplit.class.getName() + ".method(java.lang.Object,java.util.List)]",
            () -> analyzer.analyzeType(RpcWithAffinityAndSplit.class));
    }

    @Test
    public void testSplitWithoutAggregate() {
        expectExactMessage(IllegalArgumentException.class,
            "@RpcSplit can be used only in @RpcAggregate-annotated methods "
                + "[method=public abstract java.util.List " + RpcSplitWithoutAggregate.class.getName() + ".method(java.util.List)]",
            () -> analyzer.analyzeType(RpcSplitWithoutAggregate.class));
    }

    private RpcMethodInfo findMethod(String name, RpcInterfaceInfo<?> type) {
        return type.methods().stream()
            .filter(m -> m.javaMethod().getName().equals(name))
            .findFirst()
            .orElseThrow(AssertionError::new);
    }

    private void verifyRpcA(RpcInterface<TypeA> rpc) {
        assertEquals(ToString.format(rpc), rpc.toString());

        verifyTypeA(rpc.type());
    }

    private void verifyTypeA(RpcInterfaceInfo<TypeA> type) {
        assertSame(TypeA.class, type.javaType());
        assertEquals(TypeA.class.getName(), type.name());
        assertEquals(TypeA.class.getName() + ":0", type.versionedName());

        assertEquals(1, type.methods().size());
        assertTrue(type.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methA1")));
    }

    private void verifyRpcB(RpcInterface<TypeB> rpc) {
        assertEquals(ToString.format(rpc), rpc.toString());

        RpcInterfaceInfo<TypeB> type = rpc.type();

        assertSame(TypeB.class, type.javaType());
        assertEquals(TypeB.class.getName(), type.name());
        assertEquals(TypeB.class.getName() + ":1", type.versionedName());

        assertEquals(2, type.methods().size());
        assertTrue(type.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methB1")));
        assertTrue(type.methods().stream().anyMatch(m -> m.javaMethod().getName().equals("methB2")));
    }
}
