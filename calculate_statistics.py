#!/usr/bin/env python3
"""Reproduce Part 2 with analytic CDFs. Python 3, standard library only."""
import math


def originalCdf(timeMs):
    """Probability that one original has finished by timeMs after arrival."""
    # A uniform CDF is a straight line inside its support. Clamping also
    # handles times before the fastest original and after the slowest one.
    return max(0.0, min(1.0, (timeMs - 50.0) / 350.0))


def duplicatedCdf(timeMs):
    """Probability that either the original or its delayed copy has finished."""
    # Duplicates start at 200 ms and need at least another 50 ms.
    # Before 250 ms, they cannot change the original completion distribution.
    if timeMs < 250.0:
        return originalCdf(timeMs)

    # After 250 ms, a request is unfinished only if BOTH copies are unfinished.
    # Original survival is (400-t)/350. Duplicate survival is (600-t)/350,
    # because its duration is measured from the later 200 ms start.
    if timeMs < 400.0:
        return 1.0 - (400.0 - timeMs) * (600.0 - timeMs) / 350.0**2

    # The original always finishes by 400 ms, even if its duplicate is slower.
    return 1.0


def quantiles(batchSize, percentile=0.95):
    """Return baseline p95, duplicated p95, and their difference, all in ms."""
    # The batch finishes when EVERY independent request finishes, so its CDF
    # is the single-request CDF raised to batchSize. Invert that power first.
    # expm1 avoids cancellation when 1 - percentile**(1/batchSize) is tiny.
    singleRequestTail = -math.expm1(math.log(percentile) / batchSize)
    baselineMs = 1400.0 - 350.0 * singleRequestTail

    # Write d = 400 - CPU p95. The duplicate CDF gives
    # d(d+200) = 350**2 * singleRequestTail.
    # Rationalize sqrt(10000 + 350**2 * tail) - 100 to retain precision
    # rather than subtracting two numbers that are almost equal.
    duplicatedDeficitMs = 350.0**2 * singleRequestTail / (
        math.sqrt(10000.0 + 350.0**2 * singleRequestTail) + 100.0
    )
    duplicatedMs = 1400.0 - duplicatedDeficitMs

    # This closed form uses the final CDF branch. Check that assumption rather
    # than silently applying it to an unsupported percentile or batch size.
    assert 250.0 <= duplicatedMs - 1000.0 <= 400.0

    # Check the analytic inversion independently with monotone-CDF bisection.
    # Each midpoint is a candidate CPU time; the GPU is added only afterward.
    lowerMs, upperMs = 50.0, 400.0
    for _ in range(70):
        midpointMs = (lowerMs + upperMs) / 2
        if duplicatedCdf(midpointMs)**batchSize < percentile:
            lowerMs = midpointMs
        else:
            upperMs = midpointMs
    bisectedMs = 1000.0 + (lowerMs + upperMs) / 2
    assert math.isclose(duplicatedMs, bisectedMs, rel_tol=0.0, abs_tol=1e-9)

    # Finally substitute both results back into their batch CDFs. Explicitly
    # disable relative tolerance so these are the absolute checks stated here.
    assert math.isclose(
        originalCdf(baselineMs - 1000.0)**batchSize,
        percentile, rel_tol=0.0, abs_tol=1e-10,
    )
    assert math.isclose(
        duplicatedCdf(duplicatedMs - 1000.0)**batchSize,
        percentile, rel_tol=0.0, abs_tol=1e-10,
    )

    # Subtract the small CPU deficits directly. This avoids subtracting two
    # almost-identical total latencies for the 10,000-request batch.
    improvementMs = duplicatedDeficitMs - 350.0 * singleRequestTail
    return baselineMs, duplicatedMs, improvementMs


def main():
    # Both levels use exactly the same request model; only the maximum changes.
    for batchSize in (100, 10000):
        baselineMs, duplicatedMs, improvementMs = quantiles(batchSize)
        print(
            f"n={batchSize}: baseline={baselineMs:.9f} ms; "
            f"duplicates={duplicatedMs:.9f} ms; "
            f"improvement={improvementMs:.9f} ms"
        )

    # A copy is launched whenever the original exceeds 200 ms: 200/350.
    # A copy wins only when the original exceeds 250 ms. Integrating the
    # triangular winning region gives 150**2 / (2 * 350**2).
    launchProbability = 200.0 / 350.0
    winProbability = 150.0**2 / (2 * 350.0**2)
    conditionalWinProbability = winProbability / launchProbability

    # Keep the denominators visible: the final percentage conditions on launch,
    # whereas the first two are fractions of ALL original requests.
    print(f"Duplicated / all originals: {100*launchProbability:.9f}%")
    print(f"Duplicate wins / all originals: {100*winProbability:.9f}%")
    print(f"Duplicate wins / duplicated originals: {100*conditionalWinProbability:.9f}%")

    # Basic boundary checks: choosing the faster copy cannot hurt completion,
    # and the CDF must join continuously at the first possible duplicate finish.
    for timeMs in (50.0, 200.0, 250.0, 399.0, 400.0):
        assert duplicatedCdf(timeMs) >= originalCdf(timeMs)
    assert duplicatedCdf(250.0) == originalCdf(250.0)
    print("Analytic-CDF and independent bisection checks passed.")


if __name__ == "__main__":
    main()
