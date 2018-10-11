package io.hekate.util.async;

/**
 * Repeatable task.
 */
public interface RepeatingRunnable {
    /**
     * Runs this task.
     *
     * @return {@code true} if this command should be repeated; otherwise {@code false}.
     */
    boolean run();
}
