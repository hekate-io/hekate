package io.hekate.core;

/**
 * Marker interface for components that can provide a reference to the {@link Hekate} instance that those components belong to.
 */
public interface HekateSupport {
    /**
     * Returns the {@link Hekate} instance.
     *
     * @return {@link Hekate} instance.
     */
    Hekate hekate();
}
