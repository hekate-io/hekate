package io.hekate.javadoc.cluster;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.HasClusterFilter;
import org.junit.Test;

public class HasClusterFilterJavadocTest extends HekateTestBase {
    private static class FakeFilter implements HasClusterFilter<FakeFilter> {
        public void doSomething() {
            // No-op.
        }

        @Override
        public FakeFilter filterAll(ClusterFilter filter) {
            return this;
        }
    }

    @Test
    public void badAndGoodFilterExample() {
        FakeFilter cluster = new FakeFilter();
        FakeFilter filtered;

        // Start:bad_and_good_usage
        // Bad!!! (filtering is performed on every iteration).
        for (int i = 0; i < 1000; i++) {
            cluster.forRemotes().forRole("some_role").doSomething();
        }

        // Good (filtering is performed only once).
        filtered = cluster.forRemotes().forRole("some_role");
        for (int i = 0; i < 1000; i++) {
            filtered.doSomething();
        }
        // End:bad_and_good_usage
    }
}
