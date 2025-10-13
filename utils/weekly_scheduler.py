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

    min_active_days = 7 - max(0, max_rest)
    max_active_days = 7 - max(0, min_rest)
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
    rest_day_likes_range: Tuple[int, int] = (5, 15),
    rest_day_comments_range: Tuple[int, int] = (2, 8),
    rest_day_duration_range: Tuple[int, int] = (30, 120),
) -> List[Dict]:
    """
    Generate a 7-day schedule list of dicts: {dayIndex, target, isRest, method, maxLikes, maxComments, warmupDuration}.

    Strategy:
    - Randomly choose number of rest days within provided range
    - Allocate total follows randomly within bounds across active days
    - Apply 'no two high days' rule
    - Keep totals within weekly range
    - For rest days: assign method 9 (Warmup) with randomized likes/comments/duration ranges
    - For active days: assign method 1 (70%) or 4 (30%) randomly
    """
    min_week, max_week = follow_weekly_range
    min_rest, max_rest = rest_days_range
    high_day_threshold, follow_threshold = no_two_high_rule

    # Choose rest days
    rest_days_count = random.randint(max(0, min_rest), max(0, max_rest))
    rest_indices = set(_pick_rest_days(rest_days_count))

    # Determine active days and bounds
    bounds = calculate_daily_bounds(follow_weekly_range, rest_days_range, no_two_high_rule)
    daily_min = bounds["daily_min"]
    daily_max = bounds["daily_max"]
    active_days = 7 - len(rest_indices)
    active_days = max(1, active_days)

    # Pick weekly total inside range with slight bias to middle
    target_week_total = random.randint(min_week, max_week)

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
        if d in rest_indices:
            # Rest day: method 9 (Warmup) with randomized likes, comments, and duration
            max_likes = random.randint(rest_day_likes_range[0], rest_day_likes_range[1])
            max_comments = random.randint(rest_day_comments_range[0], rest_day_comments_range[1])
            warmup_duration = random.randint(rest_day_duration_range[0], rest_day_duration_range[1])
            schedule.append({
                "dayIndex": d, 
                "target": 0, 
                "isRest": True, 
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
                "method": random_method
            })

    return schedule


def validate_weekly_plan(
    generated_schedule: List[Dict],
    follow_weekly_range: Tuple[int, int],
    rest_days_range: Tuple[int, int],
    no_two_high_rule: Tuple[int, int],
) -> None:
    """Raise ValueError if constraints are violated."""
    min_week, max_week = follow_weekly_range
    min_rest, max_rest = rest_days_range
    high_day_threshold, follow_threshold = no_two_high_rule

    if len(generated_schedule) != 7:
        raise ValueError("Generated schedule must have exactly 7 days")

    total = sum(day.get("target", 0) for day in generated_schedule)
    if not (min_week <= total <= max_week):
        raise ValueError(f"Weekly total {total} out of range [{min_week}, {max_week}]")

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


