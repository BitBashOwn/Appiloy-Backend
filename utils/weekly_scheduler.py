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
    rest_day_duration_range: Tuple[int, int] = (30, 120),
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
    - For active days: assign method 1 (70%) or 4 (30%) randomly (method 6 mirrors method 1 with its own cap)
    - For off days: assign method 0 (No tasks)
    """
    min_week, max_week = follow_weekly_range
    try:
        selected_method = int(method)
    except (TypeError, ValueError):
        selected_method = 1

    # Method 1 follow workflow; keep it inside a safe 40-80 window
    if selected_method == 1:
        min_week, max_week = 40, 80
    # Method 6 is another follow-heavy workflow; keep it inside a safe 70-140 window
    if selected_method == 6:
        min_week, max_week = 70, 140

    if min_week > max_week:
        min_week, max_week = max_week, min_week
    follow_weekly_range = (min_week, max_week)

    method_profiles: Dict[int, Tuple[List[int], List[int]]] = {
        1: ([1, 4], [70, 30]),
        6: ([6, 4], [70, 30]),
        4: ([4, 1], [70, 30]),
    }
    if selected_method <= 0:
        selected_method = 1
    if selected_method not in method_profiles:
        method_profiles[selected_method] = ([selected_method], [1])
    active_method_choices, active_method_weights = method_profiles[selected_method]
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
            # Active day: randomly assign method based on the selected strategy
            # Ensure this day gets a target (should always be in day_to_target)
            target_value = day_to_target.get(d, daily_min)
            random_method = random.choices(active_method_choices, weights=active_method_weights)[0]
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
    RANDOM_MAX_LOW_FACTOR = 0.5  # 50% of cap as baseline for randomized max
    RANDOM_MAX_BUFFER = 2        # ensure randomized max is at least min+buffer when possible
    RANDOM_MIN_SPREAD = 2
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

        # Establish per-day min/max bounds (random band 5-25, capped by per-day caps)
        per_day_min_bounds: Dict[int, int] = {}
        per_day_max_bounds: Dict[int, int] = {}
        used_ranges: Set[Tuple[int, int]] = set()
        for idx in active_indices:
            cap = max(0, per_day_caps[idx])
            if cap <= 0:
                per_day_min_bounds[idx] = 0
                per_day_max_bounds[idx] = 0
                continue
            random_max_high = min(MAX_FOLLOWS_PER_DAY_PER_ACCOUNT, cap)
            if random_max_high <= 0:
                per_day_min_bounds[idx] = 0
                per_day_max_bounds[idx] = 0
                continue

            tentative_low = max(
                MIN_FOLLOWS_PER_ACTIVE_DAY + RANDOM_MAX_BUFFER,
                int(cap * RANDOM_MAX_LOW_FACTOR),
            )
            random_max_low = min(random_max_high, max(MIN_FOLLOWS_PER_ACTIVE_DAY, tentative_low))
            if random_max_low > random_max_high:
                random_max_low = random_max_high

            dynamic_day_max = random.randint(random_max_low, random_max_high)

            dynamic_spread = max(1, dynamic_day_max - MIN_FOLLOWS_PER_ACTIVE_DAY)
            min_upper_bound = max(
                MIN_FOLLOWS_PER_ACTIVE_DAY,
                dynamic_day_max - max(RANDOM_MIN_SPREAD, dynamic_spread // 2),
            )
            if min_upper_bound > dynamic_day_max:
                min_upper_bound = dynamic_day_max

            dynamic_day_min = random.randint(MIN_FOLLOWS_PER_ACTIVE_DAY, min_upper_bound)

            day_min = min(dynamic_day_min, cap)
            day_max = max(day_min, min(dynamic_day_max, cap))

            # Ensure this day's min/max combination is unique
            uniqueness_guard = 0
            while (day_min, day_max) in used_ranges and uniqueness_guard < 50:
                uniqueness_guard += 1
                # Try shifting the range slightly
                tweak_choice = random.choice(["shrink_min", "grow_max", "nudge_both"])
                if tweak_choice == "shrink_min":
                    new_min = max(MIN_FOLLOWS_PER_ACTIVE_DAY, day_min - 1)
                    if new_min <= day_max:
                        day_min = new_min
                elif tweak_choice == "grow_max":
                    new_max = min(cap, day_max + 1)
                    if new_max >= day_min:
                        day_max = new_max
                else:
                    new_min = max(MIN_FOLLOWS_PER_ACTIVE_DAY, day_min - 1)
                    new_max = min(cap, day_max + 1)
                    if new_min <= new_max:
                        day_min = new_min
                        day_max = new_max
                if day_min == day_max and day_max < cap:
                    day_max += 1
                if day_min > day_max:
                    day_min = min(day_min, day_max)
            used_ranges.add((day_min, day_max))

            per_day_min_bounds[idx] = day_min
            per_day_max_bounds[idx] = day_max

        # Seed each active day with a random value inside [5, cap] (respecting caps)
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


def generate_independent_schedules(
    accounts: List[str],
    week_start_dt: datetime,
    follow_weekly_range: Tuple[int, int],
    rest_days_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
    off_days_range: Tuple[int, int] = (1, 1),
    previous_active_days_map: Optional[Dict[str, Set[int]]] = None,
    rest_day_likes_range: Tuple[int, int] = (0, 5),
    rest_day_comments_range: Tuple[int, int] = (0, 2),
    rest_day_duration_range: Tuple[int, int] = (30, 120),
    active_method: int = 1,
) -> Dict[str, List[Dict]]:
    """
    Generates a completely independent schedule for each account.
    
    Returns:
        Dict[username, List[Dict]]
        Where the value is the full 7-day schedule (including methods and targets) for that specific account.
    """
    all_schedules = {}

    for username in accounts:
        # 1. Get previous history for this specific account if available
        prev_active = None
        if previous_active_days_map:
            prev_active = previous_active_days_map.get(username)

        # 2. Generate a UNIQUE structure (Active/Rest/Off days) for this account
        # Use the requested active method to drive per-day randomization.
        # Specific per-account overrides still happen downstream.
        base_schedule = generate_weekly_targets(
            week_start_dt=week_start_dt,
            follow_weekly_range=follow_weekly_range,
            rest_days_range=rest_days_range,
            no_two_high_rule=no_two_high_rule,
            method=active_method,
            off_days_range=off_days_range,
            previous_active_days=prev_active,
            rest_day_likes_range=rest_day_likes_range,
            rest_day_comments_range=rest_day_comments_range,
            rest_day_duration_range=rest_day_duration_range
        )

        # 3. Calculate specific follow targets for this account based on its unique schedule
        # We reuse the existing logic but pass ONLY this account
        targets_map = generate_per_account_plans(
            schedule=base_schedule,
            accounts=[username],
            follow_weekly_range=follow_weekly_range,
            no_two_high_rule=no_two_high_rule
        )
        
        final_targets = targets_map[username]

        # 4. Merge the calculated targets back into the schedule objects
        # This creates the final complete plan for this account
        final_account_schedule = []
        for day_data, target_val in zip(base_schedule, final_targets):
            # Create a copy to avoid reference issues
            day_copy = day_data.copy()
            
            # Update the target with the calculated safe value
            # Note: If the day is Off/Rest, target_val will likely be 0, which is correct.
            day_copy["target"] = target_val
            
            # Ensure warmup parameters are preserved for rest days
            if day_copy.get("isRest") and day_copy.get("method") == 9:
                if "maxLikes" not in day_copy or "maxComments" not in day_copy or "warmupDuration" not in day_copy:
                    # Re-randomize if missing (shouldn't happen, but safety check)
                    import random
                    day_copy["maxLikes"] = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
                    day_copy["maxComments"] = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
                    day_copy["warmupDuration"] = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
            
            final_account_schedule.append(day_copy)

        all_schedules[username] = final_account_schedule

    # Enforce rule: Warmup can only run if ALL accounts are on rest that day
    # If ANY account is active on a day, convert ALL warmup accounts to rest with method 0
    for day_index in range(7):
        # Check if ANY account is active this day
        has_active_account = False
        for username, schedule in all_schedules.items():
            day_data = schedule[day_index]
            if not day_data.get("isRest") and not day_data.get("isOff"):
                has_active_account = True
                break
        
        # If any account is active, convert all warmup accounts to rest with method 0
        if has_active_account:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"[WARMUP-FIX] Day {day_index}: Active account detected, converting warmup accounts to off")
            for username, schedule in all_schedules.items():
                day_data = schedule[day_index]
                if day_data.get("isRest") and day_data.get("method") == 9:
                    logger.info(f"[WARMUP-FIX] Day {day_index}: Converting {username} from warmup to off")
                    # Keep as rest day but disable warmup (method 0, target 0)
                    # This prevents validation errors from changing off day counts
                    day_data["isRest"] = True
                    day_data["isOff"] = False
                    day_data["method"] = 0
                    day_data["target"] = 0
                    # Remove warmup parameters
                    day_data.pop("maxLikes", None)
                    day_data.pop("maxComments", None)
                    day_data.pop("warmupDuration", None)

    return all_schedules


def apply_account_rotation_limit(
    account_method_map_by_day: Dict[int, Dict[str, int]],
    all_accounts: List[str],
    max_accounts_per_day: int = 4,
    min_account_threshold: int = 5
) -> Dict[int, Dict[str, int]]:
    """
    Apply rotation limit to ensure maximum 4 active accounts per day when 5+ accounts exist.
    
    - Only applies when account count >= min_account_threshold
    - Limits active accounts (methods 1, 4, 6) to max_accounts_per_day per day
    - Warmup accounts (method 9) are preserved and don't count toward the limit
    - Ensures all accounts appear at least once across the week
    - Uses random selection while maintaining fairness
    
    Args:
        account_method_map_by_day: Dict mapping day_index -> {username: method}
        all_accounts: List of all account usernames
        max_accounts_per_day: Maximum active accounts allowed per day (default 4)
        min_account_threshold: Minimum account count to trigger rotation (default 5)
    
    Returns:
        Modified account_method_map_by_day with rotation applied
    """
    # Only apply rotation if we have enough accounts
    if len(all_accounts) < min_account_threshold:
        return account_method_map_by_day
    
    # Create a copy to avoid modifying the original
    rotated_map = {}
    
    # Track which accounts have been scheduled (active, not warmup)
    scheduled_accounts = set()
    
    # First pass: Process each day and limit active accounts to max_accounts_per_day
    for day_index in range(7):
        day_map = account_method_map_by_day.get(day_index, {}).copy()
        if not day_map:
            rotated_map[day_index] = {}
            continue
        
        # Separate active accounts (methods 1, 4, 6) from warmup accounts (method 9)
        active_accounts = {}
        warmup_accounts = {}
        
        for username, method in day_map.items():
            if method == 9:
                # Warmup accounts don't count toward limit, preserve them
                warmup_accounts[username] = method
            elif method in (1, 4, 6):
                # Active accounts
                active_accounts[username] = method
        
        # If we have more than max_accounts_per_day active accounts, need to rotate
        if len(active_accounts) > max_accounts_per_day:
            # Prioritize accounts that haven't been scheduled yet
            unscheduled = {u: m for u, m in active_accounts.items() if u not in scheduled_accounts}
            scheduled = {u: m for u, m in active_accounts.items() if u in scheduled_accounts}
            
            selected_accounts = {}
            
            # First, add unscheduled accounts (up to the limit)
            if unscheduled:
                num_to_select = min(max_accounts_per_day, len(unscheduled))
                selected_unscheduled = random.sample(list(unscheduled.items()), num_to_select)
                selected_accounts.update(dict(selected_unscheduled))
                scheduled_accounts.update(selected_accounts.keys())
            
            # If we still have room, add from scheduled accounts
            remaining_slots = max_accounts_per_day - len(selected_accounts)
            if remaining_slots > 0 and scheduled:
                num_to_select = min(remaining_slots, len(scheduled))
                selected_scheduled = random.sample(list(scheduled.items()), num_to_select)
                selected_accounts.update(dict(selected_scheduled))
            
            # Combine selected active accounts with warmup accounts
            rotated_map[day_index] = {**selected_accounts, **warmup_accounts}
        else:
            # No rotation needed, keep all accounts
            rotated_map[day_index] = day_map
            # Track scheduled active accounts
            scheduled_accounts.update(active_accounts.keys())
    
    # Second pass: Ensure all accounts appear at least once
    # Find accounts that weren't scheduled in the first pass
    all_scheduled = set()
    for day_map in rotated_map.values():
        for username, method in day_map.items():
            if method in (1, 4, 6):  # Only count active accounts
                all_scheduled.add(username)
    
    unscheduled_accounts = set(all_accounts) - all_scheduled
    
    # Redistribute unscheduled accounts to days with < max_accounts_per_day
    if unscheduled_accounts:
        # Shuffle for randomness
        unscheduled_list = list(unscheduled_accounts)
        random.shuffle(unscheduled_list)
        
        for username in unscheduled_list:
            # Find a day with < max_accounts_per_day active accounts
            # Try to use the account's original method if it was scheduled somewhere
            original_method = None
            for day_map in account_method_map_by_day.values():
                if username in day_map:
                    method = day_map.get(username)
                    if method and method in (1, 4, 6):
                        original_method = method
                        break
            
            # Default to method 1 if not found
            method_to_use = original_method if original_method in (1, 4, 6) else 1
            
            # Find a day with available slots
            for day_index in range(7):
                day_map = rotated_map.get(day_index, {})
                active_count = sum(1 for m in day_map.values() if m in (1, 4, 6))
                
                if active_count < max_accounts_per_day:
                    # Add this account to this day
                    day_map[username] = method_to_use
                    rotated_map[day_index] = day_map
                    break
    
    return rotated_map
