# Workflow 5: Lead Qualification
# Scores each company profile using rule-based criteria.

SCORE_RULES = [
    ("solar installer",          40),
    ("battery storage installer", 30),
    ("commercial",                20),
]

EMPLOYEE_SCORE = 10  # bonus for estimated employee count > 20
LARGE_EMPLOYEE_THRESHOLDS = {"51-200", "200+"}

GRADE_THRESHOLDS = [
    (70, "A"),  # High value lead
    (40, "B"),  # Potential lead
    (20, "C"),  # Low priority
    (0,  "D"),  # Not relevant
]


def score_profile(profile: dict) -> dict:
    """
    Apply rule-based scoring to a company profile.

    Returns the profile dict with added fields:
      score (int), grade (str), score_breakdown (list of reasons)
    """
    score = 0
    breakdown: list[str] = []

    business_type = profile.get("business_type", "").lower()
    target_market = profile.get("target_market", "").lower()
    employee_est  = profile.get("employee_count_estimate", "unknown")

    for keyword, points in SCORE_RULES:
        if keyword in business_type or keyword in target_market:
            score += points
            breakdown.append(f"+{points} ({keyword})")

    if employee_est in LARGE_EMPLOYEE_THRESHOLDS:
        score += EMPLOYEE_SCORE
        breakdown.append(f"+{EMPLOYEE_SCORE} (employees > 20)")

    score = min(score, 100)

    grade = "D"
    for threshold, letter in GRADE_THRESHOLDS:
        if score >= threshold:
            grade = letter
            break

    return {**profile, "score": score, "grade": grade, "score_breakdown": breakdown}


def score_all_profiles(profiles: list[dict]) -> list[dict]:
    """Score a list of company profiles."""
    return [score_profile(p) for p in profiles]
