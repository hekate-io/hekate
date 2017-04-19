package io.hekate.core.internal;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;

public final class HekateNodeFactory {
    private HekateNodeFactory() {
        // No-op.
    }

    public static Hekate create(HekateBootstrap bootstrap) {
        return new HekateNode(bootstrap);
    }
}
