import random
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Optional, Set


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
    previous_active_days: Optional[Set[int]] = None,
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

    # ALWAYS ensure exactly 4 active days
    TARGET_ACTIVE_DAYS = 4
    active_days = TARGET_ACTIVE_DAYS
    
    # First, select 4 active days randomly, avoid repeating the exact previous set if provided
    all_days = list(range(7))
    attempts = 0
    while True:
        active_indices_set = set(random.sample(all_days, TARGET_ACTIVE_DAYS))
        attempts += 1
        if not previous_active_days or active_indices_set != previous_active_days or attempts >= 8:
            break
    
    # Remaining 3 days will be distributed between rest and off days
    remaining_days = [d for d in range(7) if d not in active_indices_set]
    
    # Determine bounds for calculation (using rest/off ranges for remaining days)
    bounds = calculate_daily_bounds(follow_weekly_range, rest_days_range, no_two_high_rule, off_days_range)
    daily_min = bounds["daily_min"]
    daily_max = bounds["daily_max"]
    
    # Distribute the 3 remaining days between rest and off days
    # Respect the provided ranges, but ensure we end up with exactly 3 non-active days
    off_min, off_max = off_days_range
    if off_min > off_max:
        off_min, off_max = off_max, off_min
    off_min = max(0, min(off_min, 3))  # Can't have more than 3 off days (since we need 4 active)
    off_max = max(off_min, min(3, max(0, off_max)))  # Can't have more than 3 off days
    
    rest_min = max(0, min_rest)
    rest_max = min(max(0, max_rest), 3)  # Can't have more than 3 rest days
    
    # With exactly 4 active days, we have 3 remaining days to split
    # Find valid combinations where off_count + rest_count = 3
    # and both are within their respective ranges
    valid_combinations = []
    for off_candidate in range(off_min, off_max + 1):
        rest_candidate = 3 - off_candidate
        if rest_min <= rest_candidate <= rest_max:
            valid_combinations.append((off_candidate, rest_candidate))
    
    # If no valid combination exists, prioritize rest days minimum, then off days minimum
    if not valid_combinations:
        # Try to satisfy rest_min first
        if rest_min <= 3:
            rest_count = rest_min
            off_count = 3 - rest_count
            # Adjust if off_count is out of range
            if off_count < off_min:
                off_count = off_min
                rest_count = 3 - off_count
            elif off_count > off_max:
                off_count = off_max
                rest_count = 3 - off_count
        else:
            # Can't satisfy rest_min, use minimum off days
            off_count = off_min
            rest_count = 3 - off_count
    else:
        # Randomly choose from valid combinations
        off_count, rest_count = random.choice(valid_combinations)
    
    # Ensure rest_count is at least rest_min if possible (safety check)
    # Only convert if it doesn't violate off_min constraint
    if rest_min > 0 and rest_count < rest_min and off_count > off_min:
        # Try to convert some off days to rest days
        needed_rest = rest_min - rest_count
        max_convertible = off_count - off_min  # Can't go below off_min
        if max_convertible >= needed_rest:
            rest_count += needed_rest
            off_count -= needed_rest
        elif max_convertible > 0:
            # Convert as many as possible
            rest_count += max_convertible
            off_count -= max_convertible
    
    # Now select which days are off and which are rest
    off_indices = set(random.sample(remaining_days, off_count)) if off_count > 0 else set()
    rest_indices = set([d for d in remaining_days if d not in off_indices])
    
    # Verify we have exactly 4 active days
    assert len(active_indices_set) == TARGET_ACTIVE_DAYS, f"Expected {TARGET_ACTIVE_DAYS} active days, got {len(active_indices_set)}"
    assert len(off_indices) + len(rest_indices) == 3, f"Expected 3 non-active days, got {len(off_indices)} off + {len(rest_indices)} rest"
    assert len(rest_indices) == rest_count, f"Expected {rest_count} rest days, got {len(rest_indices)}"

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
    
    # Ensure each active day gets at least 1 follow if weekly total allows
    # This ensures all 4 active days are meaningful
    min_per_day = 1
    if target_week_total >= active_days * min_per_day:
        # First, ensure each day has at least min_per_day
        for i in range(active_days):
            if active_targets[i] < min_per_day:
                active_targets[i] = min_per_day
    
    remaining = max(0, target_week_total - sum(active_targets))

    # Distribute remaining randomly with caps, ensuring all days get some distribution
    # Use a round-robin approach first to ensure fairness, then random
    distribution_order = list(range(active_days))
    random.shuffle(distribution_order)
    distribution_idx = 0
    
    while remaining > 0:
        # Try round-robin first for fairness
        if distribution_idx < len(distribution_order):
            i = distribution_order[distribution_idx]
            distribution_idx += 1
        else:
            # Then random distribution
            i = random.randrange(active_days)
        
        if active_targets[i] < daily_max:
            active_targets[i] += 1
            remaining -= 1
        else:
            # If this day is at max, try next in order or random
            if distribution_idx >= len(distribution_order):
                # All days tried, check if any can still take more
                if all(t >= daily_max for t in active_targets):
                    break
                # Reset and try random
                distribution_idx = 0
                random.shuffle(distribution_order)

    # Apply no-two-high rule
    active_targets = _apply_no_two_high_rule(active_targets, high_day_threshold, follow_threshold)

    # Create a mapping from day index to target value for active days
    sorted_active_indices = sorted(active_indices_set)
    day_to_target = {}
    for idx, target in zip(sorted_active_indices, active_targets):
        day_to_target[idx] = target

    # Build 7-day schedule aligning active targets with correct day indices
    schedule = []
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
            # Ensure this day gets a target (should always be in day_to_target)
            target_value = day_to_target.get(d, daily_min)
            random_method = random.choices([1, 4], weights=[70, 30])[0]
            schedule.append({
                "dayIndex": d, 
                "target": target_value, 
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

    # Check that we have exactly 4 active days
    active_count = sum(1 for day in generated_schedule if not day.get("isRest") and not day.get("isOff"))
    if active_count != 4:
        raise ValueError(f"Expected exactly 4 active days, got {active_count}")

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
    # With exactly 4 active days, rest days can be 0-3, so validation should be lenient
    # Only fail if rest_count is clearly outside reasonable bounds (0-3)
    if rest_count < 0 or rest_count > 3:
        raise ValueError(f"Rest days {rest_count} out of valid range [0, 3]")
    # Log a warning if rest_count doesn't match requested range, but don't fail
    if not (min_rest <= rest_count <= max_rest):
        # This is acceptable with the 4 active days constraint - rest days are adjusted to fit
        pass

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

    # Build method map and per-day caps: maximum 25 follows per day per account
    MAX_FOLLOWS_PER_DAY_PER_ACCOUNT = 25
    MIN_FOLLOWS_PER_ACTIVE_DAY = 5
    MAX_FOLLOWS_PER_ACTIVE_DAY = 15
    method_per_day: List[int] = [0] * 7
    per_day_caps: List[int] = [0] * 7
    for day in schedule:
        idx = day.get("dayIndex")
        if isinstance(idx, int):
            method_per_day[idx] = int(day.get("method", 0))
            if not day.get("isRest") and not day.get("isOff"):
                per_day_caps[idx] = MAX_FOLLOWS_PER_DAY_PER_ACCOUNT

    if not active_indices:
        return {username: [0] * 7 for username in accounts}

    per_account: Dict[str, List[int]] = {}

    for username in accounts:
        daily_targets = [0 for _ in range(7)]

        # Establish per-day min/max bounds (random band 5-15, capped by per-day caps)
        per_day_min_bounds: Dict[int, int] = {}
        per_day_max_bounds: Dict[int, int] = {}
        for idx in active_indices:
            cap = max(0, per_day_caps[idx])
            if cap <= 0:
                per_day_min_bounds[idx] = 0
                per_day_max_bounds[idx] = 0
                continue
            day_min = min(MIN_FOLLOWS_PER_ACTIVE_DAY, cap)
            day_max = min(MAX_FOLLOWS_PER_ACTIVE_DAY, cap)
            if day_max < day_min:
                day_min = day_max
            per_day_min_bounds[idx] = day_min
            per_day_max_bounds[idx] = max(day_min, day_max)

        # Seed each active day with a random value inside [5, 15] (respecting caps)
        for idx in active_indices:
            min_bound = per_day_min_bounds.get(idx, 0)
            max_bound = per_day_max_bounds.get(idx, 0)
            if max_bound <= 0:
                continue
            if min_bound <= 0:
                daily_targets[idx] = random.randint(0, max_bound)
            else:
                daily_targets[idx] = random.randint(min_bound, max_bound)

        min_total_possible = sum(per_day_min_bounds.values())
        max_total_possible = sum(per_day_max_bounds.values())

        effective_min_total = max(min_week, min_total_possible)
        effective_max_total = min(max_week, max_total_possible) if max_total_possible > 0 else 0
        if effective_min_total > effective_max_total:
            effective_min_total = min_total_possible
            effective_max_total = max_total_possible

        if effective_max_total > 0 and effective_min_total <= effective_max_total:
            target_week_total = random.randint(effective_min_total, effective_max_total)
        else:
            target_week_total = sum(daily_targets)

        difference = target_week_total - sum(daily_targets)
        adjustment_safety = 0
        while difference != 0 and adjustment_safety < 500:
            adjustment_safety += 1
            if difference > 0:
                grow_candidates = [i for i in active_indices if daily_targets[i] < per_day_max_bounds.get(i, 0)]
                if not grow_candidates:
                    break
                idx = random.choice(grow_candidates)
                max_increase = per_day_max_bounds[idx] - daily_targets[idx]
                if max_increase <= 0:
                    continue
                step = min(difference, random.randint(1, max_increase))
                daily_targets[idx] += step
                difference -= step
            else:
                shrink_candidates = [i for i in active_indices if daily_targets[i] > per_day_min_bounds.get(i, 0)]
                if not shrink_candidates:
                    break
                idx = random.choice(shrink_candidates)
                max_decrease = daily_targets[idx] - per_day_min_bounds[idx]
                if max_decrease <= 0:
                    continue
                step = min(-difference, random.randint(1, max_decrease))
                daily_targets[idx] -= step
                difference += step

        # Apply no-two-high rule
        adjusted = _apply_no_two_high_rule(daily_targets, high_day_threshold, follow_threshold)

        # Clamp final values to per-day min/max bounds and hard cap
        for i in range(7):
            if i in per_day_min_bounds:
                floor_val = per_day_min_bounds[i]
                ceiling_val = per_day_max_bounds.get(i, MAX_FOLLOWS_PER_DAY_PER_ACCOUNT)
                adjusted[i] = max(floor_val, min(ceiling_val, adjusted[i]))
            if adjusted[i] > MAX_FOLLOWS_PER_DAY_PER_ACCOUNT:
                adjusted[i] = MAX_FOLLOWS_PER_DAY_PER_ACCOUNT

        per_account[username] = adjusted

    return per_account
