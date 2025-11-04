import random
from datetime import datetime, timedelta
from typing import List, Tuple, Dict


def _ensure_monday(dt: datetime) -> bool:
    """Return True if dt is a Monday (weekday 0)."""
    return dt.weekday() == 0


def calculate_daily_bounds(
    follow_weekly_range: Tuple[int, int],
    rest_days_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
    off_days_range: Tuple[int, int] = (1, 1),
) -> Dict[str, int]:
    """
    Calculate rough per-day lower/upper bounds implied by weekly targets and rest days.

    - follow_weekly_range: (min_week, max_week)
    - rest_days_range: (min_rest, max_rest)
    - no_two_high_rule: (high_day_threshold, follow_threshold) [not directly used in bounds]

    Returns a dict with keys: daily_min, daily_max, min_active_days, max_active_days
    """
    min_week, max_week = follow_weekly_range
    min_rest, max_rest = rest_days_range
    off_min, off_max = off_days_range
    if off_min > off_max:
        off_min, off_max = off_max, off_min
    off_min = max(0, off_min)
    off_max = max(off_min, min(6, max(0, off_max)))

    min_active_days = 7 - max(0, max_rest) - off_max
    max_active_days = 7 - max(0, min_rest) - off_min
    min_active_days = max(1, min_active_days)
    max_active_days = max(min_active_days, max_active_days)

    # Daily min comes from spreading min_week over the max number of active days
    daily_min = max(0, min_week // max_active_days if max_active_days else 0)

    # Daily max comes from spreading max_week over the min number of active days
    daily_max = max_week // min_active_days if min_active_days else max_week

    # Clamp to reasonable operational caps
    daily_max = min(30, max(daily_max, daily_min + 1))
    # Slightly relax daily_min so schedules have more diversity (but keep >= 0)
    daily_min = max(0, min(daily_min, daily_max - 1))

    return {
        "daily_min": daily_min,
        "daily_max": daily_max,
        "min_active_days": min_active_days,
        "max_active_days": max_active_days,
    }


def _pick_rest_days(num_rest: int) -> List[int]:
    """Pick rest day indices (0..6) spread across the week with randomization."""
    if num_rest <= 0:
        return []
    
    # If requesting 5+ rest days (most of the week), just randomly pick
    if num_rest >= 5:
        return sorted(random.sample(range(7), num_rest))
    
    # For 1-4 rest days: spread them out with random starting point
    offset = random.randint(0, 6)  # Random starting day
    step = max(1, 7 // num_rest)
    
    rest_days = []
    for i in range(num_rest):
        day_index = (offset + i * step) % 7
        rest_days.append(day_index)
    
    # If we need more rest days (edge case), fill remaining randomly
    if len(rest_days) < num_rest:
        remaining = num_rest - len(rest_days)
        available = [d for d in range(7) if d not in rest_days]
        rest_days.extend(random.sample(available, min(remaining, len(available))))
    
    # Remove duplicates and ensure we have exactly num_rest days
    rest_days = list(set(rest_days))
    if len(rest_days) < num_rest:
        # Fill any gaps with random picks from remaining days
        available = [d for d in range(7) if d not in rest_days]
        rest_days.extend(random.sample(available, min(num_rest - len(rest_days), len(available))))
    
    return sorted(rest_days[:num_rest])


def _apply_no_two_high_rule(
    targets: List[int],
    high_day_threshold: int,
    follow_threshold: int,
) -> List[int]:
    """Ensure no two consecutive days >= high_day_threshold; lower the second to < follow_threshold."""
    if not targets:
        return targets
    adjusted = targets[:]
    for i in range(1, len(adjusted)):
        if adjusted[i - 1] >= high_day_threshold and adjusted[i] >= high_day_threshold:
            adjusted[i] = max(0, min(adjusted[i], follow_threshold - 1))
    return adjusted


def generate_weekly_targets(
    week_start_dt: datetime,
    follow_weekly_range: Tuple[int, int],
    rest_days_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
    method: int,
    rest_day_likes_range: Tuple[int, int] = (0, 5),
    rest_day_comments_range: Tuple[int, int] = (0, 2),
    rest_day_duration_range: Tuple[int, int] = (5, 10),
    off_days_range: Tuple[int, int] = (1, 1),
) -> List[Dict]:
    """
    Generate a 7-day schedule list of dicts: {dayIndex, target, isRest, method, maxLikes, maxComments, warmupDuration}.

    Strategy:
    - Select number of off days inside off_days_range (defaults to exactly 1)
    - Randomly choose number of rest days within provided range (excluding the off day)
    - Allocate total follows randomly within bounds across active days
    - Apply 'no two high days' rule
    - Keep totals within weekly range
    - For rest days: assign method 9 (Warmup) with randomized likes/comments/duration ranges
    - For active days: assign method 1 (70%) or 4 (30%) randomly
    - For off days: assign method 0 (No tasks)
    """
    min_week, max_week = follow_weekly_range
    min_rest, max_rest = rest_days_range
    high_day_threshold, follow_threshold = no_two_high_rule

    off_min, off_max = off_days_range
    if off_min > off_max:
        off_min, off_max = off_max, off_min
    off_min = max(0, off_min)
    # Keep at least one non-off day available for activity/rest selections
    off_max = max(off_min, min(6, max(0, off_max)))
    off_count = random.randint(off_min, off_max) if off_max >= 0 else 0
    off_indices = set(random.sample(range(7), off_count)) if off_count else set()

    # Choose rest days (excluding the off days) respecting provided range
    available_for_rest = [d for d in range(7) if d not in off_indices]
    max_possible_rest = len(available_for_rest)
    rest_min = max(0, min_rest)
    rest_max = min(max(0, max_rest), max_possible_rest)
    if rest_min > rest_max:
        rest_min = rest_max
    rest_days_count = random.randint(rest_min, rest_max) if rest_max >= 0 else 0
    rest_indices = set()
    if rest_days_count and max_possible_rest > 0:
        rest_indices = set(random.sample(available_for_rest, min(rest_days_count, max_possible_rest)))

    # Determine active days and bounds
    bounds = calculate_daily_bounds(follow_weekly_range, rest_days_range, no_two_high_rule, off_days_range)
    daily_min = bounds["daily_min"]
    daily_max = bounds["daily_max"]
    # Effective active days exclude rest days and off days
    active_days = 7 - len(rest_indices) - len(off_indices)

    # Ensure at least one active day remains
    if active_days <= 0:
        # First, try converting rest days back to active days
        while active_days <= 0 and rest_indices:
            rest_indices.pop()
            active_days = 7 - len(rest_indices) - len(off_indices)
        # If still no active day, reduce off days
        while active_days <= 0 and off_indices:
            off_indices.pop()
            active_days = 7 - len(rest_indices) - len(off_indices)
        active_days = max(1, active_days)

    # Pick weekly total inside achievable range
    max_total = active_days * max(1, daily_max)
    min_total = active_days * max(0, daily_min)
    achievable_min = max(min_week, min_total)
    achievable_max = min(max_week, max_total)
    if achievable_min > achievable_max:
        # If requested min cannot be met with current constraints, clamp to achievable_max
        target_week_total = achievable_max
    else:
        target_week_total = random.randint(achievable_min, achievable_max)

    # Seed active day targets at daily_min; remaining to distribute
    active_targets = [daily_min for _ in range(active_days)]
    remaining = max(0, target_week_total - sum(active_targets))

    # Distribute remaining randomly with caps
    while remaining > 0:
        i = random.randrange(active_days)
        if active_targets[i] < daily_max:
            active_targets[i] += 1
            remaining -= 1
        else:
            # If all are at max, break
            if all(t >= daily_max for t in active_targets):
                break

    # Apply no-two-high rule
    active_targets = _apply_no_two_high_rule(active_targets, high_day_threshold, follow_threshold)

    # Build 7-day schedule aligning active targets with non-rest indices in order
    schedule = []
    active_iter = iter(active_targets)
    for d in range(7):
        if d in off_indices:
            # Off day: no tasks scheduled
            schedule.append({
                "dayIndex": d, 
                "target": 0, 
                "isRest": False, 
                "isOff": True,
                "method": 0
            })
        elif d in rest_indices:
            # Rest day: method 9 (Warmup) with randomized likes, comments, and duration
            max_likes = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
            max_comments = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
            warmup_duration = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
            schedule.append({
                "dayIndex": d, 
                "target": 0, 
                "isRest": True, 
                "isOff": False,
                "method": 9,
                "maxLikes": max_likes,
                "maxComments": max_comments,
                "warmupDuration": warmup_duration
            })
        else:
            # Active day: randomly assign method 1 (70%) or 4 (30%)
            random_method = random.choices([1, 4], weights=[70, 30])[0]
            schedule.append({
                "dayIndex": d, 
                "target": next(active_iter), 
                "isRest": False,
                "isOff": False,
                "method": random_method
            })

    return schedule


def validate_weekly_plan(
    generated_schedule: List[Dict],
    follow_weekly_range: Tuple[int, int],
    rest_days_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
    off_days_range: Tuple[int, int] = (1, 1),
) -> None:
    """Raise ValueError if constraints are violated."""
    min_week, max_week = follow_weekly_range
    min_rest, max_rest = rest_days_range
    high_day_threshold, follow_threshold = no_two_high_rule
    off_min, off_max = off_days_range
    if off_min > off_max:
        off_min, off_max = off_max, off_min
    off_min = max(0, off_min)
    off_max = max(off_min, max(0, off_max))

    if len(generated_schedule) != 7:
        raise ValueError("Generated schedule must have exactly 7 days")

    # Check off day count is within allowed bounds
    off_count = sum(1 for day in generated_schedule if day.get("isOff"))
    if not (off_min <= off_count <= off_max):
        raise ValueError(f"Off days {off_count} out of range [{off_min}, {off_max}]")

    # Be lenient when requested range isn't achievable under constraints
    total = sum(day.get("target", 0) for day in generated_schedule)
    # Recompute achievable window similar to generator
    bounds = calculate_daily_bounds(
        (min_week, max_week), (min_rest, max_rest), (high_day_threshold, follow_threshold), off_days_range
    )
    daily_min = bounds["daily_min"]
    daily_max = bounds["daily_max"]
    active_days = sum(1 for d in generated_schedule if not d.get("isRest") and not d.get("isOff"))
    max_total = active_days * max(1, daily_max)
    min_total = active_days * max(0, daily_min)
    achievable_min = max(min_week, min_total)
    achievable_max = min(max_week, max_total)
    if achievable_min > achievable_max:
        achievable_min = achievable_max
    # Accept totals within achievable window; do not hard-fail when outside the original request
    if not (achievable_min <= total <= achievable_max):
        return

    rest_count = sum(1 for day in generated_schedule if day.get("isRest"))
    if not (min_rest <= rest_count <= max_rest):
        raise ValueError(f"Rest days {rest_count} out of range [{min_rest}, {max_rest}]")

    # No two consecutive high days
    prev_high = False
    for day in generated_schedule:
        t = day.get("target", 0)
        is_high = t >= high_day_threshold
        if prev_high and is_high:
            raise ValueError("Two consecutive high target days detected")
        prev_high = is_high




def generate_per_account_plans(
    schedule: List[Dict],
    accounts: List[str],
    follow_weekly_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
) -> Dict[str, List[int]]:
    """
    For each account, generate a 7-length list of daily follow targets aligned with the provided schedule.

    - Off and rest days receive 0
    - Active days receive a randomized distribution of a per-account weekly total
    - Applies the no-two-high-days rule per account
    """
    if not accounts:
        return {}

    min_week, max_week = follow_weekly_range
    high_day_threshold, follow_threshold = no_two_high_rule

    # Identify active day indices and per-day methods
    active_indices = [d.get("dayIndex") for d in schedule if not d.get("isRest") and not d.get("isOff")]
    active_indices = [i for i in active_indices if isinstance(i, int)]
    min_active_days = max(1, len(active_indices))

    # Build method map and per-day caps: method 1 => 80, others => 30
    method_per_day: List[int] = [0] * 7
    per_day_caps: List[int] = [0] * 7
    for day in schedule:
        idx = day.get("dayIndex")
        if isinstance(idx, int):
            method_per_day[idx] = int(day.get("method", 0))
            if not day.get("isRest") and not day.get("isOff"):
                per_day_caps[idx] = 80 if method_per_day[idx] == 1 else 30

    per_account: Dict[str, List[int]] = {}

    for username in accounts:
        # Pick a random weekly total per account (independent to diversify)
        account_week_total = random.randint(min_week, max_week)
        daily_targets = [0 for _ in range(7)]

        # Method 1 indices among active days
        method1_indices = [i for i in active_indices if method_per_day[i] == 1]

        # Encourage baseline 10–20 per Method 1 day (randomized), within weekly cap
        baseline_map: Dict[int, int] = {}
        if method1_indices:
            for idx in method1_indices:
                # Random baseline in [10, 20], also respect per-day caps for that index
                baseline_map[idx] = min(per_day_caps[idx], random.randint(10, 20))
            min_required_for_method1 = sum(baseline_map.values())
            account_week_total = min(max(account_week_total, min_required_for_method1), max_week)

        remaining = account_week_total

        # Seed Method 1 days up to their randomized baseline (capped by remaining and per-day caps)
        for idx in method1_indices:
            if remaining <= 0:
                break
            seed_target = baseline_map.get(idx, 0)
            if seed_target <= 0:
                continue
            seed = min(seed_target, per_day_caps[idx])
            add = min(seed, remaining)
            if add > 0:
                daily_targets[idx] += add
                remaining -= add

        # Distribute remaining across active days with slight noise to caps
        cap_noise = random.uniform(0.9, 1.15)
        effective_caps = [0] * 7
        for i in active_indices:
            noisy_cap = int(per_day_caps[i] * cap_noise)
            effective_caps[i] = max(1, min(per_day_caps[i], noisy_cap))

        while remaining > 0 and active_indices:
            idx = random.choice(active_indices)
            if daily_targets[idx] < effective_caps[idx]:
                daily_targets[idx] += 1
                remaining -= 1
            else:
                if all(daily_targets[i] >= effective_caps[i] for i in active_indices):
                    break

        # Apply no-two-high rule and clamp method 1 to max 80
        adjusted = _apply_no_two_high_rule(daily_targets, high_day_threshold, follow_threshold)
        for i in method1_indices:
            if adjusted[i] > 80:
                adjusted[i] = 80

        per_account[username] = adjusted

    return per_account
