"""
Synthetic healthcare claims data generator.
Produces 1000 realistic medical claims records and saves to data/raw/claims.csv.
"""

import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

# --- Reference data pools ---

# Common ICD-10 diagnosis codes (description omitted; code only)
ICD10_CODES = [
    "E11.9",   # Type 2 diabetes without complications
    "I10",     # Essential hypertension
    "J06.9",   # Acute upper respiratory infection
    "M54.5",   # Low back pain
    "F32.9",   # Major depressive disorder
    "J18.9",   # Pneumonia
    "N39.0",   # Urinary tract infection
    "K21.0",   # GERD with esophagitis
    "I25.10",  # Coronary artery disease
    "J44.1",   # COPD with acute exacerbation
    "E78.5",   # Hyperlipidemia
    "M17.11",  # Primary osteoarthritis, right knee
    "F41.1",   # Generalized anxiety disorder
    "Z23",     # Immunization encounter
    "Z00.00",  # General adult medical exam
    "G43.909", # Migraine
    "R05.9",   # Cough
    "R51.9",   # Headache
    "Z12.31",  # Encounter for colorectal cancer screening
    "S62.001A",# Fracture of navicular bone of wrist
]

# Common CPT procedure codes
CPT_CODES = [
    "99213",  # Office visit, established, low complexity
    "99214",  # Office visit, established, moderate complexity
    "99203",  # Office visit, new, low complexity
    "99204",  # Office visit, new, moderate complexity
    "99283",  # ED visit, moderate severity
    "93000",  # ECG with interpretation
    "85025",  # Complete blood count (CBC)
    "80053",  # Comprehensive metabolic panel
    "71046",  # Chest X-ray, 2 views
    "45378",  # Colonoscopy, diagnostic
    "36415",  # Routine venipuncture
    "90686",  # Influenza vaccine, quadrivalent
    "99232",  # Subsequent hospital care
    "27447",  # Total knee arthroplasty
    "43239",  # Upper GI endoscopy with biopsy
    "70553",  # MRI brain with contrast
    "72148",  # MRI lumbar spine without contrast
    "99395",  # Preventive visit, established, 18-39 years
    "99396",  # Preventive visit, established, 40-64 years
    "81003",  # Urinalysis, automated
]

CLAIM_STATUSES = ["Paid", "Denied", "Pending", "Appealed", "Adjusted"]
STATUS_WEIGHTS  = [0.60,   0.15,    0.12,      0.07,       0.06]

INSURANCE_TYPES = ["Commercial", "Medicare", "Medicaid", "Medicare Advantage", "Self-Pay"]
INSURANCE_WEIGHTS = [0.45,        0.25,       0.15,       0.10,                 0.05]

GENDERS = ["M", "F"]

# Typical billed amounts by CPT (base ± variance applied later)
CPT_BASE_AMOUNTS = {
    "99213": 150, "99214": 220, "99203": 180, "99204": 260, "99283": 800,
    "93000": 90,  "85025": 30,  "80053": 50,  "71046": 200, "45378": 1200,
    "36415": 25,  "90686": 45,  "99232": 350, "27447": 18000, "43239": 1500,
    "70553": 2200, "72148": 1800, "99395": 250, "99396": 280, "81003": 20,
}

# Paid ratio by status
PAID_RATIO = {
    "Paid": (0.70, 0.95),
    "Denied": (0.0, 0.0),
    "Pending": (0.0, 0.0),
    "Appealed": (0.0, 0.50),
    "Adjusted": (0.50, 0.85),
}


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _dob_for_insurance(insurance_type: str) -> date:
    today = date.today()
    if insurance_type in ("Medicare", "Medicare Advantage"):
        # Age 65-89
        age = random.randint(65, 89)
    elif insurance_type == "Medicaid":
        # Broader range, skewed younger
        age = random.randint(0, 64)
    else:
        age = random.randint(18, 70)
    birth_year = today.year - age
    try:
        dob = date(birth_year, random.randint(1, 12), random.randint(1, 28))
    except ValueError:
        dob = date(birth_year, 1, 1)
    return dob


def generate_claims(n: int = 1000) -> list[dict]:
    service_start = date(2023, 1, 1)
    service_end   = date(2024, 12, 31)

    # Pre-generate stable patient and provider pools
    patient_ids  = [f"PAT{str(i).zfill(5)}" for i in range(1, 201)]
    provider_ids = [f"PRV{str(i).zfill(4)}" for i in range(1, 51)]

    records = []
    for _ in range(n):
        claim_id      = f"CLM{uuid.uuid4().hex[:10].upper()}"
        patient_id    = random.choice(patient_ids)
        provider_id   = random.choice(provider_ids)
        service_date  = _random_date(service_start, service_end)
        diagnosis_code  = random.choice(ICD10_CODES)
        procedure_code  = random.choice(CPT_CODES)
        insurance_type  = random.choices(INSURANCE_TYPES, weights=INSURANCE_WEIGHTS)[0]
        claim_status    = random.choices(CLAIM_STATUSES,  weights=STATUS_WEIGHTS)[0]
        member_gender   = random.choice(GENDERS)
        member_dob      = _dob_for_insurance(insurance_type)

        base = CPT_BASE_AMOUNTS.get(procedure_code, 200)
        claim_amount = round(base * random.uniform(0.80, 1.30), 2)

        lo, hi = PAID_RATIO[claim_status]
        if lo == hi == 0.0:
            paid_amount = 0.00
        else:
            paid_amount = round(claim_amount * random.uniform(lo, hi), 2)

        records.append({
            "claim_id":       claim_id,
            "patient_id":     patient_id,
            "provider_id":    provider_id,
            "service_date":   service_date.isoformat(),
            "diagnosis_code": diagnosis_code,
            "procedure_code": procedure_code,
            "claim_amount":   claim_amount,
            "paid_amount":    paid_amount,
            "claim_status":   claim_status,
            "insurance_type": insurance_type,
            "member_dob":     member_dob.isoformat(),
            "member_gender":  member_gender,
        })

    return records


def main():
    output_path = Path(__file__).resolve().parents[2] / "data" / "raw" / "claims.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records = generate_claims(1000)

    fieldnames = [
        "claim_id", "patient_id", "provider_id", "service_date",
        "diagnosis_code", "procedure_code", "claim_amount", "paid_amount",
        "claim_status", "insurance_type", "member_dob", "member_gender",
    ]

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {len(records)} claims → {output_path}")


if __name__ == "__main__":
    main()
