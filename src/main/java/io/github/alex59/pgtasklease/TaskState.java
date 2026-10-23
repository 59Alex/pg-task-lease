package io.github.alex59.pgtasklease;

/** Durable queue states. DEAD means the configured attempt budget was exhausted. */
public enum TaskState {
    /** Available for claim after its scheduled time. */
    READY,
    /** Temporarily owned, until its deadline unless renewed. */
    LEASED,
    /** Successfully acknowledged. */
    DONE,
    /** Attempt budget exhausted. */
    DEAD
}
