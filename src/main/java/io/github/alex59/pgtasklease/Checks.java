package io.github.alex59.pgtasklease;

import java.time.Duration;
import java.util.Objects;

final class Checks {
    private Checks() { }

    static void text(String value, String name, int max) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > max) {
            throw new IllegalArgumentException(name + " must have 1.." + max + " non-blank characters");
        }
    }

    static long millis(Duration value, String name, boolean positive) {
        Objects.requireNonNull(value, name);
        if (value.isNegative() || value.compareTo(Duration.ofDays(365)) > 0
                || (positive && value.compareTo(Duration.ofMillis(1)) < 0)
                || value.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException(name + " must be whole milliseconds, "
                    + (positive ? "positive" : "non-negative") + ", at most 365 days");
        }
        return value.toMillis();
    }
}
