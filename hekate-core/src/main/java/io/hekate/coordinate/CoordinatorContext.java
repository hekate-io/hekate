package io.hekate.coordinate;

/**
 * Coordinator context.
 *
 * @see CoordinationHandler#coordinate(CoordinatorContext)
 */
public interface CoordinatorContext extends CoordinationContext {
    /**
     * Completes this coordination process.
     */
    void complete();
}
