# Databricks Senior DE Interview — Gold Prep

**Context (from HR):** Panel is 4 Polish engineers + 1 Indian (Sidram Kotenor). Their favourite topics: **Databricks Asset Bundles, Unity Catalog, Delta Live Tables, migration, Metastore**. Question format: 2 questions — **one Python coding, one SQL query**. Python may be array/two-pointer/sliding-window or time-intervals flavor. Trapping Rain Water is on the shortlist. SQL is likely window functions or running-window / gaps-and-islands.

**This doc is your one-stop-shop.** Read it once end-to-end, drill the coding sections twice, rehearse the intro out loud three times.

---

## TABLE OF CONTENTS

1. [Interview overview & panel intel](#1-interview-overview--panel-intel)
2. [Self-introduction — 2 to 3 minutes](#2-selfintroduction--2-to-3-minutes)
3. [Project walkthrough — 2 to 3 minutes](#3-project-walkthrough--2-to-3-minutes)
4. [Python coding prep](#4-python-coding-prep)
   - 4A. Two pointers pattern
   - 4B. Sliding window pattern
   - 4C. Time intervals pattern
   - 4D. DP medium warmups
   - 4E. Trapping Rain Water — deep dive
5. [SQL coding prep](#5-sql-coding-prep)
6. [Databricks deep dives](#6-databricks-deep-dives)
   - 6A. Unity Catalog + Metastore
   - 6B. Delta Live Tables (a.k.a. Lakeflow Declarative Pipelines)
   - 6C. Databricks Asset Bundles (DABs)
   - 6D. Migration patterns
7. [Behavioural & scenario prep](#7-behavioural--scenario-prep)
8. [Day-of checklist](#8-dayof-checklist)
9. [Post-interview](#9-postinterview)

---

## 1. Interview overview & panel intel

**Likely company profile.** Polish + Indian mixed panel, Databricks-first, EMEA-based — high probability this is a Kraków/Warsaw office of a consulting firm (Accenture, Capco) OR a product company's Polish DE team (Heineken has a Databricks/BI team in Kraków; HSBC and JPMC both have big DE teams there). Either way, the tech is going to be **Azure Databricks + Delta Lake + Unity Catalog + Databricks Asset Bundles**.

**Format expectation (based on HR):**
- **~5 min** self-intro + project pitch
- **~20 min** SQL problem (medium difficulty, window functions likely)
- **~20 min** Python problem (medium DSA, array-flavor)
- **~10 min** deep-dive discussion on Databricks (their favourite topics)
- **~5 min** your questions

**Panel-specific tells:**
- Their favourites are **DABs, Unity Catalog, DLT, migration, Metastore**. Every one of your resume bullets touches these. Prepare a 60-sec deep-dive per topic.
- The Polish DE community leans heavily toward **Azure Databricks**. Expect Azure-specific terms (ADLS Gen2, Azure Data Factory, Azure Purview, Azure Key Vault, service principals).
- Sidram Kotenor being on the panel suggests they hire India → Poland or work in a distributed org. Not the tech to focus on; just be professional.

---

## 2. Self-introduction — 2 to 3 minutes

Rehearse this out loud 3-5 times before the interview. **Time yourself** — you should hit 2:15-2:45.

> "Hi, I'm Saket. I'm a Senior Data Engineer with 6+ years across Snowflake, BigQuery, and Databricks. I'm based in Bengaluru.
>
> I started my career at Infosys, where I spent about two years on the Walmart account building Spark-Scala and PySpark ETL pipelines running on **Databricks and GCP DataProc** — ingesting massive datasets from GCS into BigQuery with automated data-quality gates. I also built an AES-256/PGP encryption framework for a second client, Five Below, processing 80 GB+ of PII data across Python, PySpark, Scala, and Java.
>
> I then moved to Falabella — the largest e-commerce platform in Latin America — as a Data Engineer working on their Chile marketplace. My flagship project there was a Fast Shipping Tags data product covering 4 million+ SKUs on BigQuery, DataProc, and Pub/Sub — it drove a 50% lift in platform conversion rates. I also built a serverless ingestion framework on Cloud Functions and a custom Airflow observability platform monitoring 2,000+ DAGs, which improved MTTR by 60%.
>
> Currently, I'm the **founding data engineer on the Product Rating platform at New Relic**. My biggest project has been leading a migration of 440+ dbt models off Snowflake onto an open lakehouse — Apache Iceberg on S3, Spark, Airflow 3. Along the way I've delivered $45K+ in annual Snowflake credit savings via dbt refactoring, cut deployment lead time by 85% with a Jenkins/GitHub CI/CD framework, and open-sourced a compile-time SQL-dialect transpiler called **dbt-polyglot** on PyPI — it lets Snowflake-authored dbt models execute on Spark and Databricks unchanged.
>
> Outside the day job, I maintain a 58-module Data Engineering curriculum called **lakehouse-lab** — Spark, Iceberg, Delta Lake, Kafka, Debezium CDC, dbt, and Airflow, all laptop-safe.
>
> I'm here because this role would give me the chance to bring the migration and tooling experience I've built into a Databricks-native environment, which is a stack I already have production experience with from Infosys and OSS work through dbt-polyglot targeting Databricks Spark."

**Notes on delivery:**
- Say it flat, not sing-song. Confident, not hurried.
- Pause 1 sec between company transitions. Gives the interviewer time to absorb.
- If they cut you off mid-sentence — good sign, they found their hook. Follow their lead.

---

## 3. Project walkthrough — 2 to 3 minutes

If asked *"Tell me about your most recent Databricks-related project,"* pivot to this. **Do not lie about using Databricks at NR** — use the honest framing.

> "The most Databricks-adjacent project I'm running is the Snowflake → open lakehouse migration at New Relic. Let me walk you through the architecture and how it maps to Databricks.
>
> **Source:** Snowflake — 440 dbt models across staging, intermediate, marts. Billing-critical, rating engine sits at the top.
>
> **Target:** open lakehouse — Apache Iceberg on S3 for storage, Spark on our internal cluster for compute, Project Nessie as the catalog with git-style branching, Airflow 3 for orchestration, and dbt-spark as the transformation layer.
>
> **On Databricks, this same architecture would be:** Delta Lake (or Iceberg via UniForm) on ADLS, Serverless Pro SQL Warehouse for the dbt-databricks target with Photon on, Unity Catalog for the three-level namespace and governance, and Databricks Workflows for orchestration. Everything I'm doing is portable — my `dbt-polyglot` project actually treats Spark and Databricks Spark as first-class targets, so the SQL layer runs unchanged.
>
> **The three interesting challenges:**
>
> **First — the dialect chasm.** Snowflake has `QUALIFY`, `IFF`, `NVL`, `::` casts, `DATEADD`, opposite NULL ordering — Spark rejects all of them. Options were: rewrite every model (weeks of work, high bug risk), maintain two versions (double the surface area), or transparent transpilation. I built dbt-polyglot to solve the third — it hooks into dbt's compile phase, parses the model as Snowflake SQL, transpiles to Spark SQL via sqlglot, adds a Spark correctness fix-up layer, and dbt executes the Spark SQL. Model `.sql` files are never edited.
>
> **Second — migration safety.** For a billing pipeline, silent-wrong is the worst outcome. I built a parity framework: for each model, we run both Snowflake and Spark versions, then diff — row counts, column-level checksums, sampled row-level comparison. Results land in an audit table with a mismatch-spike dashboard breaking green/amber/red per model. On Databricks the equivalent would be DLT expectations plus a Databricks Workflow doing the dual-run.
>
> **Third — zero-downtime cutover.** Blue/green at the view layer. Downstream consumers read from `serving.*` views. For each model, we dual-run for a validation window, then flip the view to point at the new lakehouse target when parity has been green for N days. On Databricks I'd do this with Unity Catalog dynamic views.
>
> The project is ~65% complete; I've been running it since late 2024. The lessons — especially around cross-warehouse validation and the seed-driven monetization pattern — port directly to a Databricks context, which is why I'm particularly interested in this role."

**Delivery: ~2:30 pacing. Rehearse aloud.**

---

## 4. Python coding prep

**Universal advice for the coding round:**
1. **Restate the problem back** in your own words. Confirm inputs, outputs, edge cases.
2. **Ask about constraints** — array size, value range, negatives, empty input, duplicates.
3. **State the brute force first**, give its complexity, then say "we can do better."
4. **State the optimal approach in one sentence** before coding.
5. **Talk while typing**. Silence = red flag.
6. **Trace through a small example** after coding.
7. **State time + space complexity** at the end unprompted.

---

### 4A. Two Pointers pattern

**When to use:** sorted arrays; pair-finding; palindrome; partitioning; problems that shrink a range from both ends.

**Template:**

```python
def two_pointer_template(arr):
    left, right = 0, len(arr) - 1
    while left < right:
        # process arr[left] and arr[right]
        if some_condition:
            left += 1
        else:
            right -= 1
    return result
```

#### Problem 1 — Two Sum II (sorted input)

```python
def two_sum(numbers, target):
    left, right = 0, len(numbers) - 1
    while left < right:
        s = numbers[left] + numbers[right]
        if s == target:
            return [left + 1, right + 1]  # 1-indexed
        elif s < target:
            left += 1
        else:
            right -= 1
    return []
```

Time O(n), space O(1). Interview line: *"The array is sorted, so I can move pointers based on whether the sum is too low or too high."*

#### Problem 2 — Container With Most Water

```python
def max_area(height):
    left, right = 0, len(height) - 1
    best = 0
    while left < right:
        area = (right - left) * min(height[left], height[right])
        best = max(best, area)
        if height[left] < height[right]:
            left += 1
        else:
            right -= 1
    return best
```

Time O(n), space O(1). Interview line: *"Moving the shorter side is the only move that can produce a larger area — moving the taller side can only shrink the width."*

#### Problem 3 — 3Sum

```python
def three_sum(nums):
    nums.sort()
    result = []
    n = len(nums)
    for i in range(n - 2):
        if i > 0 and nums[i] == nums[i - 1]:
            continue  # skip duplicates for i
        left, right = i + 1, n - 1
        while left < right:
            s = nums[i] + nums[left] + nums[right]
            if s == 0:
                result.append([nums[i], nums[left], nums[right]])
                # skip duplicates
                while left < right and nums[left] == nums[left + 1]:
                    left += 1
                while left < right and nums[right] == nums[right - 1]:
                    right -= 1
                left += 1
                right -= 1
            elif s < 0:
                left += 1
            else:
                right -= 1
    return result
```

Time O(n²), space O(1) excluding output. Watch for the deduplication logic — that's the trap.

---

### 4B. Sliding Window pattern

**When to use:** substrings/subarrays with a "size-K" or "at most K distinct" constraint.

**Template (variable window):**

```python
def sliding_window_template(s, k):
    left = 0
    window_state = {}      # or set, or count
    best = 0
    for right in range(len(s)):
        # expand right into the window
        window_state[s[right]] = window_state.get(s[right], 0) + 1

        # shrink from the left while invariant is broken
        while invariant_broken(window_state):
            window_state[s[left]] -= 1
            if window_state[s[left]] == 0:
                del window_state[s[left]]
            left += 1

        # update result
        best = max(best, right - left + 1)
    return best
```

#### Problem 1 — Longest Substring Without Repeating Characters (LC 3)

```python
def length_of_longest_substring(s):
    seen = {}
    left = 0
    best = 0
    for right, ch in enumerate(s):
        if ch in seen and seen[ch] >= left:
            left = seen[ch] + 1
        seen[ch] = right
        best = max(best, right - left + 1)
    return best
```

Time O(n), space O(min(n, alphabet)).

#### Problem 2 — Longest Repeating Character Replacement (LC 424)

```python
def character_replacement(s, k):
    count = {}
    left = 0
    max_freq = 0
    best = 0
    for right, ch in enumerate(s):
        count[ch] = count.get(ch, 0) + 1
        max_freq = max(max_freq, count[ch])
        # window size - max_freq = chars to replace
        while (right - left + 1) - max_freq > k:
            count[s[left]] -= 1
            left += 1
        best = max(best, right - left + 1)
    return best
```

Interview line: *"The window is valid if (window_length - most_frequent_char_count) ≤ k. When it stops being valid, shrink from the left."*

#### Problem 3 — Minimum Window Substring (LC 76) — hard but classic

```python
from collections import Counter

def min_window(s, t):
    if not t or not s: return ""
    need = Counter(t)
    missing = len(t)
    left = 0
    best_start, best_len = 0, float('inf')
    for right, ch in enumerate(s):
        if need[ch] > 0:
            missing -= 1
        need[ch] -= 1
        while missing == 0:
            if right - left + 1 < best_len:
                best_start, best_len = left, right - left + 1
            need[s[left]] += 1
            if need[s[left]] > 0:
                missing += 1
            left += 1
    return "" if best_len == float('inf') else s[best_start : best_start + best_len]
```

Time O(|s| + |t|), space O(|t|).

---

### 4C. Time Intervals pattern (HR flagged this)

**When to use:** meetings, ranges, schedules, sessionization.

**Universal first step:** sort by start time (or end time — pick based on problem).

#### Problem 1 — Merge Intervals (LC 56)

```python
def merge(intervals):
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    for start, end in intervals[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged
```

Time O(n log n), space O(n) for output. Interview line: *"Sort by start, then walk through — if the current interval starts before or when the last merged one ended, extend it; otherwise start a new one."*

#### Problem 2 — Meeting Rooms II (LC 253) — the classic min-heap problem

```python
import heapq

def min_meeting_rooms(intervals):
    if not intervals: return 0
    intervals.sort(key=lambda x: x[0])
    rooms = []  # min-heap of end times
    for start, end in intervals:
        if rooms and rooms[0] <= start:
            heapq.heappop(rooms)   # room freed up
        heapq.heappush(rooms, end)
    return len(rooms)
```

Time O(n log n), space O(n). Interview line: *"For every new meeting, check if the earliest-ending room is free. If yes, reuse it; else open a new room. The heap gives us the earliest end in O(log n)."*

#### Problem 3 — Insert Interval (LC 57)

```python
def insert(intervals, new_interval):
    result = []
    i, n = 0, len(intervals)
    # 1. add all intervals ending before new_interval starts
    while i < n and intervals[i][1] < new_interval[0]:
        result.append(intervals[i])
        i += 1
    # 2. merge overlapping intervals into new_interval
    while i < n and intervals[i][0] <= new_interval[1]:
        new_interval[0] = min(new_interval[0], intervals[i][0])
        new_interval[1] = max(new_interval[1], intervals[i][1])
        i += 1
    result.append(new_interval)
    # 3. add remaining intervals
    while i < n:
        result.append(intervals[i])
        i += 1
    return result
```

Time O(n), space O(n) for output.

#### Problem 4 — Non-overlapping Intervals (LC 435) — greedy

```python
def erase_overlap_intervals(intervals):
    if not intervals: return 0
    intervals.sort(key=lambda x: x[1])   # sort by END, not start
    prev_end = intervals[0][1]
    kept = 1
    for start, end in intervals[1:]:
        if start >= prev_end:
            kept += 1
            prev_end = end
    return len(intervals) - kept
```

Interview line: *"Sort by end time. Keep every interval that starts at or after the previous kept one's end. This is the classic 'activity selection' greedy."*

---

### 4D. DP medium warmups

Small chance you get DP but HR flagged medium DP. Have these three loaded.

#### House Robber (LC 198)

```python
def rob(nums):
    prev, curr = 0, 0
    for n in nums:
        prev, curr = curr, max(curr, prev + n)
    return curr
```

Space O(1). Interview line: *"At each house, I either take it (curr = prev + n) or skip it (curr stays)."*

#### Coin Change (LC 322)

```python
def coin_change(coins, amount):
    dp = [float('inf')] * (amount + 1)
    dp[0] = 0
    for a in range(1, amount + 1):
        for c in coins:
            if c <= a:
                dp[a] = min(dp[a], dp[a - c] + 1)
    return dp[amount] if dp[amount] != float('inf') else -1
```

Time O(amount × len(coins)), space O(amount).

#### Longest Increasing Subsequence (LC 300)

```python
from bisect import bisect_left

def length_of_lis(nums):
    tails = []
    for n in nums:
        i = bisect_left(tails, n)
        if i == len(tails):
            tails.append(n)
        else:
            tails[i] = n
    return len(tails)
```

Time O(n log n). If they want O(n²) DP:

```python
def length_of_lis_dp(nums):
    n = len(nums)
    dp = [1] * n
    for i in range(n):
        for j in range(i):
            if nums[j] < nums[i]:
                dp[i] = max(dp[i], dp[j] + 1)
    return max(dp)
```

---

### 4E. Trapping Rain Water — deep dive (LC 42)

**This is the flagged one. Master all three approaches. Interview will ask you to explain intuition, then code the optimal.**

**Problem restatement:**
> "Given an array of non-negative integers representing an elevation map where each bar has width 1, compute how much water can be trapped after raining."

Example: `[0,1,0,2,1,0,1,3,2,1,2,1]` → 6.

**Core insight:** Water at position `i` = `min(max_height_to_left, max_height_to_right) - height[i]`, clamped at 0.

#### Approach 1 — Precompute left/right maxes (O(n) time, O(n) space)

```python
def trap_precompute(height):
    if not height: return 0
    n = len(height)
    left_max = [0] * n
    right_max = [0] * n
    left_max[0] = height[0]
    for i in range(1, n):
        left_max[i] = max(left_max[i - 1], height[i])
    right_max[n - 1] = height[n - 1]
    for i in range(n - 2, -1, -1):
        right_max[i] = max(right_max[i + 1], height[i])
    return sum(min(left_max[i], right_max[i]) - height[i] for i in range(n))
```

**Interview line:** *"Straightforward brute-force optimization. Two linear passes for the maxes, one for the sum. O(n) time, O(n) space."*

#### Approach 2 — Two pointers (O(n) time, O(1) space) ← the one to code in the interview

```python
def trap(height):
    if not height: return 0
    left, right = 0, len(height) - 1
    left_max = right_max = 0
    water = 0
    while left < right:
        if height[left] < height[right]:
            if height[left] >= left_max:
                left_max = height[left]
            else:
                water += left_max - height[left]
            left += 1
        else:
            if height[right] >= right_max:
                right_max = height[right]
            else:
                water += right_max - height[right]
            right -= 1
    return water
```

**Interview intuition (say this):**
> "Water level at any position is bounded by the smaller of the two max heights around it. So I keep two pointers and two running maxes. At each step, I look at the smaller of the two heights. If it's `height[left]`, then I know `left_max` is the bottleneck for water at position `left` — because whatever `right_max` is, it's at least `height[right]`, which is bigger. So I can safely compute trapped water using `left_max` alone."

**Trace it on `[0,1,0,2,1,0,1,3,2,1,2,1]`:**
- l=0(0), r=11(1): h[l]<h[r], left_max=0, no water, l→1
- l=1(1), r=11(1): equal, right_max=1, no water, r→10
- l=1(1), r=10(2): h[l]<h[r], left_max=1, no water, l→2
- l=2(0), r=10(2): h[l]<h[r], water += 1-0 = 1, l→3
- l=3(2), r=10(2): equal, right_max=2, no water, r→9
- l=3(2), r=9(1): h[r]<h[l], water += 2-1 = 1 (total 2), r→8
- l=3(2), r=8(2): equal, right_max=2, no water, r→7
- l=3(2), r=7(3): h[l]<h[r], left_max=2, no water, l→4
- l=4(1), r=7(3): water += 2-1 = 1 (total 3), l→5
- l=5(0), r=7(3): water += 2-0 = 2 (total 5), l→6
- l=6(1), r=7(3): water += 2-1 = 1 (total 6), l→7. Stop.

Result: **6** ✓.

#### Approach 3 — Monotonic stack (O(n) time, O(n) space)

```python
def trap_stack(height):
    stack = []
    water = 0
    for i, h in enumerate(height):
        while stack and height[stack[-1]] < h:
            bottom = stack.pop()
            if not stack:
                break
            left = stack[-1]
            width = i - left - 1
            bounded_height = min(height[left], h) - height[bottom]
            water += width * bounded_height
        stack.append(i)
    return water
```

Interview line: *"Monotonic decreasing stack of indices. When a bar breaks the monotone, we pop and compute the water trapped in the valley."*

**Follow-up questions to expect:**
- *"What if the array is huge and doesn't fit in memory?"* → Stream in chunks, keep running left_max. Right_max is harder — you'd need a two-pass over disk or approximate.
- *"2D version — LC 407 Trapping Rain Water II?"* → Priority-queue BFS starting from the borders, always process the cell with the lowest wall height. Skip if they don't push you here — LC 407 is genuinely hard.

---

## 5. SQL coding prep

**HR flagged: window functions + running window.** These are the patterns.

### 5A. Window function anatomy (memorize this)

```sql
<function>(<expr>) OVER (
    PARTITION BY <cols>       -- optional; groups rows
    ORDER BY <cols>           -- optional but needed for ranking / running aggregates
    ROWS BETWEEN <a> AND <b>  -- optional frame; controls the running window
)
```

**Frame syntax:**
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — running total (from start of partition to current row)
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` — 7-day moving average (current + 6 back)
- `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` — running total from current to end
- Default when `ORDER BY` is given: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`

**Function cheat sheet:**

| Function | What it does | Example |
|---|---|---|
| `ROW_NUMBER()` | 1, 2, 3, 4 — unique | Top-N per group |
| `RANK()` | 1, 2, 2, 4 — gaps on ties | Ranking with ties allowed |
| `DENSE_RANK()` | 1, 2, 2, 3 — no gaps | Ranking without gaps |
| `NTILE(n)` | Buckets rows into n groups | Percentile bucketing |
| `LAG(col, k)` | Value from k rows before | Detect changes / gaps |
| `LEAD(col, k)` | Value from k rows after | Sessionization |
| `SUM/AVG/COUNT() OVER(...)` | Running / windowed aggregate | Cumulative sales |
| `FIRST_VALUE(col)` / `LAST_VALUE(col)` | First / last in frame | Anchor value |

---

### 5B. Running total (most common interview problem)

**Prompt:** "For each customer, show cumulative sales by order date."

```sql
SELECT
    customer_id,
    order_date,
    sales_amount,
    SUM(sales_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales
ORDER BY customer_id, order_date;
```

**Talking point:** *"UNBOUNDED PRECEDING TO CURRENT ROW is the frame that gives me a running total. PARTITION BY resets it per customer."*

---

### 5C. Rolling 7-day moving average

**Prompt:** "For each product, show a 7-day moving average of daily sales."

```sql
SELECT
    product_id,
    sales_date,
    daily_sales,
    AVG(daily_sales) OVER (
        PARTITION BY product_id
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM daily_sales;
```

**Trap:** if some dates are missing (no sales that day), this returns the average of the 7 nearest *rows*, not the 7 nearest *dates*. Handle with a **date spine** (LEFT JOIN a `generate_series` of dates against the sales table with COALESCE-to-zero).

---

### 5D. Top-N per group

**Prompt:** "Get the top 3 highest-paid employees per department."

```sql
WITH ranked AS (
    SELECT
        employee_id,
        department_id,
        salary,
        DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rk
    FROM employees
)
SELECT employee_id, department_id, salary
FROM ranked
WHERE rk <= 3;
```

**Interview point:** *"DENSE_RANK if ties should share a rank without gaps; ROW_NUMBER if I need to break ties arbitrarily and want exactly 3 rows."*

---

### 5E. Gaps and Islands — the classic pattern

**Prompt:** "Find consecutive-day login streaks per user."

The trick: subtract a `ROW_NUMBER()` from the date. Rows in the same streak get the same difference.

```sql
WITH numbered AS (
    SELECT
        user_id,
        login_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
    FROM logins
),
grouped AS (
    SELECT
        user_id,
        login_date,
        DATE_SUB(login_date, INTERVAL rn DAY) AS streak_group
    FROM numbered
)
SELECT
    user_id,
    MIN(login_date) AS streak_start,
    MAX(login_date) AS streak_end,
    COUNT(*) AS streak_length
FROM grouped
GROUP BY user_id, streak_group
ORDER BY user_id, streak_start;
```

**Interview line:** *"The trick is `date - row_number`. For consecutive dates, this difference is constant. So grouping by that constant gives me each streak."*

Alternative pattern using `LAG`:

```sql
WITH marked AS (
    SELECT
        user_id,
        login_date,
        CASE WHEN DATE_DIFF(login_date, LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date), DAY) > 1
             OR LAG(login_date) OVER (PARTITION BY user_id ORDER BY login_date) IS NULL
             THEN 1 ELSE 0 END AS is_new_streak
    FROM logins
),
groups AS (
    SELECT
        user_id,
        login_date,
        SUM(is_new_streak) OVER (PARTITION BY user_id ORDER BY login_date) AS grp
    FROM marked
)
SELECT user_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS len
FROM groups
GROUP BY user_id, grp;
```

---

### 5F. Sessionization (30-minute inactivity)

**Prompt:** "Group events into sessions — a new session starts if there's more than 30 minutes of inactivity."

```sql
WITH gapped AS (
    SELECT
        user_id,
        event_ts,
        CASE
            WHEN TIMESTAMP_DIFF(event_ts, LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts), MINUTE) > 30
                 OR LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL
            THEN 1 ELSE 0
        END AS new_session
    FROM events
),
sessions AS (
    SELECT
        user_id,
        event_ts,
        SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_ts) AS session_id
    FROM gapped
)
SELECT user_id, session_id, MIN(event_ts) AS session_start, MAX(event_ts) AS session_end, COUNT(*) AS n_events
FROM sessions
GROUP BY user_id, session_id;
```

**Interview line:** *"Two-step pattern — mark session boundaries with a CASE using LAG, then running-sum the boundary flags to derive session IDs. Common template."*

---

### 5G. Compare vs previous row (change detection)

**Prompt:** "For each product, show the daily percentage change in sales."

```sql
SELECT
    product_id,
    sales_date,
    daily_sales,
    LAG(daily_sales) OVER (PARTITION BY product_id ORDER BY sales_date) AS prev_day_sales,
    ROUND(
        100.0 * (daily_sales - LAG(daily_sales) OVER (PARTITION BY product_id ORDER BY sales_date))
        / NULLIF(LAG(daily_sales) OVER (PARTITION BY product_id ORDER BY sales_date), 0),
        2
    ) AS pct_change
FROM daily_sales
ORDER BY product_id, sales_date;
```

`NULLIF(x, 0)` guards against divide-by-zero.

---

## 6. Databricks deep dives

**These are the panel's favourite topics. Master them.**

### 6A. Unity Catalog + Metastore

**What it is:** Unity Catalog (UC) is Databricks' unified governance layer, GA'd in 2022. It centralizes catalog, schema, table, view, volume, function, and model definitions across workspaces. Replaces the legacy **Hive Metastore** (HMS) which was per-workspace and had a two-level namespace.

**The three-level namespace:**

```
metastore
└── catalog          (top-level container — usually per env: dev / staging / prod)
    └── schema       (aka "database" — logical grouping: sales, marketing)
        └── object   (table / view / materialized_view / volume / function / model)
```

Object reference: `catalog.schema.object_name`. Example: `prod.finance.transactions`.

**Compare to the old world:**

| Old (Hive Metastore, ~2020) | New (Unity Catalog, 2026) |
|---|---|
| Two-level: `schema.table` | Three-level: `catalog.schema.object` |
| Per-workspace metastore | One metastore per region, shared across workspaces |
| Workspace-local groups | Account-level identities (users, groups, service principals) |
| No cross-workspace sharing | Delta Sharing built-in |
| GRANT/REVOKE at cluster ACL level | Fine-grained ACLs at object level; row-level filters; column masks |
| `dbutils.fs.mount()` for cloud paths | External Locations + Storage Credentials + Volumes |
| No lineage | Column-level lineage across notebooks / jobs / DLT |
| Manual audit | System tables: `system.access.audit`, `system.billing.usage`, `system.query.history` |

**Managed vs external tables:**
- **Managed table:** UC owns both the metadata AND the underlying storage. `DROP` deletes both. Storage is in a UC-managed location.
- **External table:** UC owns the metadata; you own the storage path via an External Location. `DROP` only removes the metadata; files stay.

**Volumes:** UC-governed access to arbitrary files (CSV, JSON, images, ML artefacts). Replaces DBFS mounts for regulated environments. Reference: `/Volumes/<catalog>/<schema>/<volume>/path/to/file`.

**Permissions model:**

```sql
GRANT SELECT ON TABLE prod.finance.transactions TO `data_analysts`;
GRANT USAGE ON SCHEMA prod.finance TO `data_analysts`;
GRANT USAGE ON CATALOG prod TO `data_analysts`;
```

Notice: to access a table, the principal needs `USAGE` on the catalog AND schema, plus `SELECT` on the table.

**Row-level security + column masks:**

```sql
-- Row filter — restrict rows by user attribute
CREATE FUNCTION region_filter(region STRING) RETURN
  is_account_group_member('emea_team') AND region = 'EMEA';

ALTER TABLE prod.sales.orders SET ROW FILTER region_filter ON (region);

-- Column mask — obscure PII for non-privileged users
CREATE FUNCTION mask_email(email STRING) RETURN
  CASE WHEN is_account_group_member('pii_readers') THEN email
       ELSE CONCAT('***@', SPLIT(email, '@')[1]) END;

ALTER TABLE prod.customers.users ALTER COLUMN email SET MASK mask_email;
```

**Migration from Hive Metastore to Unity Catalog:**

1. **Provision UC prerequisites:** metastore, storage credentials (IAM role on AWS / managed identity on Azure), external locations pointing at your cloud storage paths.
2. **Convert identities:** workspace-local groups → account-level groups. Users and service principals become account-level.
3. **Attach workspaces** to the metastore.
4. **Use the UCX tool** (open-source assessment + migration helper published by Databricks Labs) — it audits your existing HMS content, identifies compatibility issues, and generates migration scripts.
5. **Upgrade tables** with `SYNC` (Databricks-provided command that clones HMS table metadata into UC):

    ```sql
    SYNC SCHEMA hive_metastore.legacy_db AS main.legacy_db;
    ```

6. **Grant UC permissions** to the account-level identities. Old cluster ACLs don't carry over.
7. **Update client code** to use three-level names: `main.legacy_db.orders` instead of `legacy_db.orders`.
8. **Sunset HMS** — mark tables read-only, then eventually drop.

**In the interview, they may ask:**
- *"Walk me through UC-enabling a workspace with a lot of HMS tables."* → the 8 steps above.
- *"How do you handle service accounts for automated jobs?"* → Databricks service principals (SPs) at the account level, granted USAGE + specific object grants.
- *"How does external table access work under the hood?"* → UC has a Storage Credential (IAM role or managed identity) linked to an External Location (cloud path prefix); permissions are checked at query time, and short-lived credentials are vended to the compute.

---

### 6B. Delta Live Tables (a.k.a. Lakeflow Declarative Pipelines)

**What it is:** DLT (rebranded to **Lakeflow Declarative Pipelines** in 2025 but the DLT syntax and concepts are unchanged) is a declarative pipeline framework — you define target tables + their transformations + data-quality expectations, and DLT figures out the DAG, dependency order, retries, backfill, incremental refresh, and monitoring.

**Two kinds of DLT tables:**

1. **Streaming table** — incremental, one-row-per-microbatch semantics. Sources are typically streams (Auto Loader, Kafka) or append-only sources.
2. **Materialized view** — recomputed on refresh. Best for aggregations. Databricks Lakeflow now supports **incremental refresh** where DLT figures out how to only recompute changed rows (2025 feature).

**Simple pipeline:**

```python
import dlt
from pyspark.sql.functions import *

# Bronze — raw ingest
@dlt.table(comment="Raw events from S3 landing zone")
def bronze_events():
    return (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", "/Volumes/prod/checkpoints/schema")
              .load("/Volumes/prod/raw/events/"))

# Silver — cleaned + validated
@dlt.table(comment="Cleaned events")
@dlt.expect_or_drop("valid_ts", "event_ts IS NOT NULL")
@dlt.expect_or_fail("known_action", "action IN ('click','view','purchase')")
def silver_events():
    return dlt.read_stream("bronze_events").filter("action != 'debug'")

# Gold — aggregation as materialized view
@dlt.table(comment="Daily action counts")
def gold_action_counts():
    return (dlt.read("silver_events")
              .groupBy(to_date("event_ts").alias("d"), "action")
              .count())
```

**Expectations (data quality):**

- `@dlt.expect("name", "condition")` — record violations, pass the row through.
- `@dlt.expect_or_drop(...)` — drop the row on violation.
- `@dlt.expect_or_fail(...)` — abort the pipeline on violation.

**CDC ingestion with `apply_changes`:**

```python
dlt.create_streaming_table("silver_customers")

dlt.apply_changes(
    target = "silver_customers",
    source = "bronze_customer_events",
    keys = ["customer_id"],
    sequence_by = "event_ts",
    apply_as_deletes = "op = 'd'",
    stored_as_scd_type = 1,   # or 2 for SCD-2 history
)
```

This handles inserts, updates, deletes from a CDC feed (Debezium, Fivetran, etc.) — no manual `MERGE` code needed.

**Pipeline lifecycle:**
- **Development mode** — retries fast, doesn't delete resources on failure. For iterative dev.
- **Production mode** — robust retries, full teardown. Runs on scheduled trigger.

**Compute:** DLT runs on its own compute pool. **Serverless DLT compute** is now the default recommendation (2025) — faster startup, better autoscaling, less admin.

**Interview-tier questions on DLT:**
- *"When would you NOT use DLT?"* → When you need arbitrary Python between stages (DLT works best with declarative table definitions), or when your orchestration has heavy cross-system dependencies (external APIs, custom retry logic).
- *"DLT vs Databricks Workflows?"* → DLT is declarative table transformations; Workflows is imperative task orchestration. Common pattern: use DLT for the transformation DAG, Workflows for scheduling/glue.
- *"How does DLT handle backfill?"* → Full refresh mode reprocesses the entire source; incremental refresh only picks up new data via checkpoint semantics.

---

### 6C. Databricks Asset Bundles (DABs)

**What it is:** DABs (GA'd 2024, major direct-deploy-engine upgrade in 2025) are Databricks' Infrastructure-as-Code. You describe Databricks resources (jobs, DLT pipelines, notebooks, ML endpoints, DLT-Lakeflow pipelines, Genie spaces, Vector Search endpoints, etc.) as YAML source files and deploy them across environments (`dev`, `staging`, `prod`) via the Databricks CLI. Replaces the older `dbx` tool.

**Bundle structure:**

```
my-project/
├── databricks.yml           # bundle root config
├── resources/
│   ├── jobs.yml
│   └── pipelines.yml
├── src/
│   ├── etl.py
│   └── notebooks/
└── tests/
```

**`databricks.yml` example (with 2025 features):**

```yaml
bundle:
  name: product-rating-etl

variables:
  warehouse_id:
    description: "SQL Warehouse for dbt runs"
    default: "abc123"

resources:
  jobs:
    dbt_prod:
      name: dbt Production Run
      tasks:
        - task_key: dbt_build
          dbt_task:
            project_directory: ./dbt
            commands:
              - "dbt deps"
              - "dbt build --target prod"
            warehouse_id: ${var.warehouse_id}
          job_cluster_key: main

      job_clusters:
        - job_cluster_key: main
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_D4s_v3"
            num_workers: 2

  pipelines:
    silver_events:
      name: Silver Events DLT
      target: prod.analytics
      libraries:
        - notebook:
            path: ./src/dlt/silver_events.py
      serverless: true         # 2025: serverless DLT is default

targets:
  dev:
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    variables:
      warehouse_id: "dev-warehouse-id"

  prod:
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    mode: production           # enforces prod safety (no dev-mode overrides)
    variables:
      warehouse_id: "prod-warehouse-id"
```

**CLI commands (v2 CLI required):**

```bash
databricks bundle validate                    # syntax + schema check
databricks bundle deploy -t dev               # deploy to dev target
databricks bundle deploy -t prod              # deploy to prod
databricks bundle run -t prod dbt_prod        # run a job
databricks bundle destroy -t dev              # tear down dev resources
databricks bundle summary                     # what's deployed
databricks bundle deploy -t dev --plan-only   # 2025: preview changes before apply
```

**What changed in 2025:**
- **Direct Deployment Engine (GA)** — replaces the Terraform-based deployment engine that DABs used under the hood. Faster deploys, no Terraform state issues.
- **`--plan-only` / `--plan` flag** — dry-run to preview changes before applying (like `terraform plan`).
- **Genie spaces, Vector Search endpoints, Postgres branches** — now first-class DABs resources.
- **Custom bundle templates** — orgs can publish org-specific bundle templates.

**CI/CD integration** (standard pattern):

```yaml
# .github/workflows/deploy.yml
name: Deploy Databricks Bundle

on:
  push:
    branches: [main]
  pull_request:

jobs:
  deploy-dev:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ vars.DEV_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DEV_TOKEN }}
      - run: databricks bundle deploy -t dev

  deploy-prod:
    runs-on: ubuntu-latest
    needs: [deploy-dev]
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ vars.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
```

**Interview-tier questions on DABs:**
- *"How is DABs different from Terraform?"* → DABs is Databricks-specific and higher-level (defines Databricks resources with domain semantics). Terraform is generic infra. Many orgs use Terraform for cloud infrastructure (VPC, IAM) and DABs for Databricks assets on top.
- *"How do you handle secrets in bundles?"* → Reference Databricks secret scopes; never inline secrets in YAML. `${secrets.scope.key}` interpolation.
- *"Preview environments per PR?"* → Yes — use `${bundle.git.branch}` in target overrides to create `pr_{branch}` schemas / job names.
- *"Bundle mode: development vs production?"* → `mode: development` (default for dev targets) prefixes resource names with the user, so multiple devs can deploy in parallel without collision. `mode: production` enforces strict deployment (no user prefix, must be the specified service principal, etc.).

---

### 6D. Migration patterns

Three migrations you might get asked about:

#### D.1 — Hive Metastore → Unity Catalog

Covered in section 6A. Short version:

1. Provision UC (metastore, storage credentials, external locations).
2. Move identities to account level.
3. Attach workspaces.
4. Assess with UCX tool.
5. `SYNC` schemas from `hive_metastore.*` to `main.*`.
6. Grant UC permissions.
7. Update three-level names in code.
8. Sunset HMS.

#### D.2 — Snowflake → Databricks (this is your NR story)

**Architecture-level moves:**

| Snowflake | Databricks equivalent |
|---|---|
| Warehouse (X-Small…4XL) | SQL Warehouse (2X-Small…4X-Large + Serverless/Pro/Classic) |
| Database.schema.table | Catalog.schema.table (Unity Catalog) |
| `IFF`, `NVL`, `QUALIFY`, `::` | Spark SQL equivalents (see dbt-polyglot) |
| Streams + Tasks | Structured Streaming / DLT with `apply_changes` |
| Snowpipe | Auto Loader (`cloudFiles`) |
| Time Travel (90-day default) | Delta time travel (default 30 days via `logRetentionDuration`) |
| Zero-copy Cloning | `CREATE TABLE ... DEEP CLONE` or `SHALLOW CLONE` |
| Role hierarchy | Account-level groups + object-level GRANT |
| `WAREHOUSE_SIZE` autoscale | Cluster autoscaling min/max |
| Snowsight dashboards | AI/BI Dashboards |
| Snowflake credits | DBUs + cloud VM cost |

**Migration mechanics:**

1. **Stand up the target** — UC metastore, external locations, catalogs `dev` / `prod`, an ingestion SQL Warehouse.
2. **Data movement** — for one-shot: `COPY INTO` from Snowflake staged Parquet exports to Delta. For ongoing: dual-write from source systems, or lakehouse-federation reads.
3. **Model rewrite** — dbt models. Use `dbt-databricks` adapter. Snowflake dialect gaps handled by dbt-polyglot or manual translation.
4. **Parity validation** — dual-run + row/column checksums. This is your NR story.
5. **View-swap cutover** — `serving.*` views point at Snowflake, flip to Databricks per-model as parity clears.
6. **Sunset Snowflake** — read-only, then decommission.

#### D.3 — DBFS mounts → Unity Catalog Volumes

Legacy: `dbutils.fs.mount("s3a://bucket/", "/mnt/data/", ...)`.

Modern: Create External Location + Volume:

```sql
CREATE EXTERNAL LOCATION my_bucket_loc
  URL 's3://bucket/'
  WITH (STORAGE CREDENTIAL my_credential);

CREATE EXTERNAL VOLUME prod.raw.data
  LOCATION 's3://bucket/raw-data/';
```

Then reference paths as `/Volumes/prod/raw/data/some_file.parquet`. Permissions via `GRANT READ VOLUME`.

---

## 7. Behavioural & scenario prep

**Behavioural — STAR format.** Have 3 stories ready:

1. **Conflict / disagreement:** "The team debated Iceberg vs Delta for the migration. I built a decision matrix on multi-engine access, cost of switching, mature-of-tooling. We chose Iceberg for engine portability. Six months in, still the right call."
2. **Failure / setback:** "Early in the migration I underestimated the null-ordering divergence between Snowflake and Spark. A rating calculation produced silently different results. Root cause: `ORDER BY` semantics. Fix: dbt-polyglot's NULLS-LAST semantic preservation. Lesson: correctness fixtures need to happen BEFORE the model migration, not after."
3. **Leadership / mentorship:** "As founding DE on a 2 → 7 team, I set the code standards, wrote the CI/CD framework, and pair-programmed with the first 4 hires on their first dbt PRs. Their PR review comments are now the training material."

**Scenario questions (common for senior DE):**

- *"How would you migrate a 2 TB Snowflake table to Delta with minimal downtime?"* → COPY INTO to staging Delta location, dual-write for validation period, view-swap.
- *"Your DLT pipeline is failing intermittently at 3 AM. Debug approach?"* → Check DLT event log (`event_log(pipeline_id)`), Structured Streaming checkpoints, cluster driver logs, upstream data quality (schema drift, sudden volume spikes).
- *"Cost blowup on a SQL Warehouse — what do you check first?"* → `system.query.history` — long-running queries, unnecessarily large results, missing WHERE / partition predicates, warehouse over-sized for workload.
- *"How do you enforce data contracts?"* → dbt `contracts:` in model YAML + Unity Catalog table constraints (NOT NULL, CHECK) + DLT expectations on ingestion.

---

## 8. Day-of checklist

**Two days before:**
- [ ] Reread this doc end-to-end
- [ ] Reread the three "-basics" companion docs (study guide, framing, alternate universe)
- [ ] Rehearse self-intro out loud 3 times, timed
- [ ] Rehearse project walkthrough out loud 3 times, timed
- [ ] Code Trapping Rain Water from scratch (blank editor, no reference) — twice
- [ ] Code Merge Intervals + Meeting Rooms II from scratch — twice
- [ ] Write a running-total SQL query + gaps-and-islands query from scratch — twice
- [ ] Look up the panel on LinkedIn, note their current role + tenure — helps you name-drop naturally
- [ ] Research the company: their engineering blog, careers page, recent GitHub activity, any Databricks case studies they've published

**Morning of:**
- [ ] Reread the "master 2-sentence answer" from the framing doc
- [ ] Skim your resume
- [ ] Water on desk, phone silenced, camera framed at eye level, clean background
- [ ] Test screen-share (they may ask you to code in a shared editor — CoderPad, HackerRank Live, or their internal tool)

**During the interview:**
- [ ] **Restate the problem** before starting to code
- [ ] **Talk while typing** — silence is a red flag
- [ ] **Trace through a small example** after coding
- [ ] **State time + space complexity** unprompted
- [ ] **Ask questions** at the end — see suggested list below

**Suggested questions to ask them:**
- "How do you use Unity Catalog — one metastore across environments, or per-env?"
- "Are you on Delta natively, or Iceberg via UniForm — and did the Tabular acquisition change your thinking?"
- "Where does DLT / Lakeflow fit vs Workflows in your architecture?"
- "How do you handle migrations from Hive Metastore — have you finished, or is it ongoing?"
- "What's the split between DABs and Terraform in your team?"
- "How do you handle preview environments per PR?"
- "Which recent Databricks Data + AI Summit talk influenced how you build?"

Asking any TWO of these signals: you're current, you're strategic, you're the right level.

---

## 9. Post-interview

Send a same-day thank-you note. **3 sentences max.** Reference ONE specific thing the interviewer said (proves you were listening + engaged).

Template:
> Hi [Name],
>
> Thanks for taking the time today. I especially enjoyed the discussion on [specific topic they raised — e.g. "your Unity Catalog rollout strategy across dev/prod"]. If it would be useful, happy to send you a link to my `dbt-polyglot` PyPI project we touched on — it's the SQL-dialect transpiler I mentioned.
>
> Looking forward to the next steps.
>
> Best,
> Saket

Don't oversell. Don't restate your resume. Just be professional and human.

---

## Companion docs (all in `~/Documents/docs/personal/`)

- `databricks-senior-de-study-guide.md` — the technical reference (v1 for expert readers)
- `databricks-senior-de-study-guide-basics.md` — same content, rewritten assuming your Databricks knowledge is 5 years stale (**read this first if you haven't seriously used Databricks since ~2020**)
- `nr-work-as-databricks-framing.md` — interview positioning of your NR work (expert framing)
- `nr-work-as-databricks-framing-basics.md` — same, with feature primers for rust-off users
- `nr-alternate-universe-databricks.md` — hypothetical "how I'd have built NR on Databricks" (expert)
- `nr-alternate-universe-databricks-basics.md` — same, basics-friendly
- **This file** — one-stop gold prep for the specific interview

---

**You've got this. Go be crisp.**
