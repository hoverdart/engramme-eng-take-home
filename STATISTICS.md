# Part 2: Batch latency and duplicate requests

All times below are in milliseconds, measured from the original batch arrival. The GPU adds a fixed 1,000 ms after the last CPU result. The calculations use the stated independent uniform durations and unlimited CPU capacity; they do not model contention or network effects.

## 1. Baseline: 100 users

Let each original preprocessing duration be `X_i ~ Uniform(50, 400)`, independently. Its CDF is

```text
F_X(t) = 0                       for t < 50
         (t - 50) / 350          for 50 <= t <= 400
         1                       for t > 400.
```

The CPU batch completion time is `M_n = max(X_1, ..., X_n)`. Independence gives

```text
P(M_n <= t) = product_i P(X_i <= t) = F_X(t)^n.
```

The full batch latency `L_n = 1000 + M_n` has CDF `F_X(l - 1000)^n`. Setting that CDF to 0.95 gives

```text
p95(L_n) = 1000 + 50 + 350 * 0.95^(1/n).
```

For **n = 100**, the baseline p95 is **1399.820520 ms**.

## 2. Duplicate unfinished requests at 200 ms

For one original request, let `D ~ Uniform(50, 400)` be an independent potential duplicate duration. A duplicate is launched only if `X > 200`. The effective completion time is

```text
T = X                       if X <= 200
    min(X, 200 + D)         if X > 200.
```

The 200 ms offset matters: even the fastest duplicate cannot complete before **250 ms**. For the calculation, `T = min(X, 200 + D)` also works for every request: a potential duplicate can never beat an original that finishes by 200 ms. The actual policy still launches copies only for unfinished requests.

For `250 <= t < 400`, the request is unfinished exactly when both `X > t` and `D > t - 200`. Since these durations are independent,

```text
P(T > t) = ((400 - t) / 350) * ((600 - t) / 350).
```

The full CDF is therefore

```text
F_T(t) = 0                                              for t < 50
         (t - 50) / 350                                 for 50 <= t < 250
         1 - ((400 - t)(600 - t)) / 122500               for 250 <= t < 400
         1                                              for t >= 400.
```

The effective completion times remain independent across requests, because each uses its own independent pair `(X_i, D_i)`. The duplicated batch latency has CDF `F_T(l - 1000)^n`.

Let `a_n = 0.95^(1/n)` and `e_n = 1 - a_n`. For both batch sizes in this exercise the CPU p95 is in `[250, 400]`. Solving `F_T(t) = a_n` gives

```text
(400 - t)(600 - t) = 122500 * e_n
CPU p95 = 500 - sqrt(10000 + 122500 * e_n)
batch p95 = 1500 - sqrt(10000 + 122500 * e_n).
```

For **n = 100**:

| Quantity | Milliseconds |
| --- | ---: |
| Baseline batch p95 | 1399.820520 |
| Batch p95 with duplicates | 1399.686401 |
| Improvement | **0.134119** |

### How often are duplicates launched and useful?

**Duplicate launched, denominator: all original requests.**

```text
P(X > 200) = (400 - 200) / 350 = 4/7 = 57.142857%.
```

**Duplicate wins, denominator: all original requests.**

A duplicate can win only if the original lasts longer than 250 ms. Conditioning on the original duration `x`, it wins with probability `(x - 250) / 350` for `250 < x <= 400`. Hence

```text
P(200 + D < X)
  = integral from 250 to 400 of [(x - 250) / 350] * [1 / 350] dx
  = 150^2 / (2 * 350^2)
  = 9/98
  = 9.183673%.
```

This event already implies that a duplicate was launched. Ties have probability zero.

**Duplicate wins, denominator: requests that receive a duplicate.**

```text
P(duplicate wins | X > 200)
  = (9/98) / (4/7)
  = 9/56
  = 16.071429%.
```

For 100 originals, the expected counts are 57.143 launched duplicates and 9.184 duplicate winners. The percentages are probabilities for an individual request, not probabilities that the entire batch benefits.

## 3. A single batch of 10,000 users

Substitute `n = 10000` in the same maximum-CDF formulas:

| Quantity | Milliseconds |
| --- | ---: |
| Baseline batch p95 | 1399.998205 |
| Batch p95 with duplicates | 1399.996858 |
| Improvement | **0.001346** |

The three percentages are unchanged: **57.142857% of all originals are duplicated**, **9.183673% of all originals have a duplicate win**, and **16.071429% of duplicated originals have a duplicate win**. Batch size changes the number of independent request pairs and the maximum, but not the duration distribution or the duplicate policy for any individual request. The expected counts are now 5714.286 launches and 918.367 duplicate winners.

### Why the benefit shrinks

Both policies have the same 400 ms upper endpoint for CPU completion. Even with duplicates there is a positive probability of a request finishing arbitrarily close to 400 ms: its original is slow and its duplicate does not finish earlier. As the batch grows, the maximum approaches that endpoint under both policies, so both full-batch p95 values approach 1400 ms.

More precisely, write `d = 400 - t`. Near the endpoint,

```text
Baseline single-request tail = d / 350
Duplicated single-request tail = d(d + 200) / 122500.
```

With `c = -log(0.95)` and large `n`, `e_n ~ c/n`. The baseline p95 deficit below 1400 ms is approximately `350*c/n`; the duplicated deficit is approximately `612.5*c/n`. The improvement is therefore approximately `262.5*c/n`, which tends to zero and explains the roughly 100-fold reduction when the batch grows from 100 to 10,000.

### Recommendation

Under these assumptions, duplicating every unfinished request at 200 ms offers little practical benefit for batch p95: about **0.134 ms** for 100 users and **0.00135 ms** for 10,000. It launches **57.14% more preprocessing requests**. That is an increase in request starts, not necessarily the same percentage increase in CPU work: actual work depends on whether losing copies are canceled and how quickly cancellation takes effect.

I would **not implement this strategy for this workload and objective** unless a specific latency requirement made these tiny gains valuable. The model grants enough CPU that duplicates never slow originals, so contention is not part of the calculation. In a real deployment, extra capacity, cancellation, and contention would need measurement. Hedging may be worth testing for a different workload with a long or unbounded tail, especially one where rare stragglers dominate latency; that would be a different probability model.

## Reproduce the numbers

Run from the repository root:

```bash
python3 calculate_statistics.py
```

The script uses only Python's standard library and exact analytic CDFs, with double-precision numerical evaluation. It uses `expm1` and a rationalized square-root expression to avoid cancellation for large batches. It independently checks the duplicated quantiles with CDF bisection and verifies that both quantiles reproduce a batch CDF of 0.95. No simulation is used, so there is no Monte Carlo sampling error. Results are reported to six decimal places in milliseconds; the script prints nine.
