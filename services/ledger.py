from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import List


def _safe_float(value) -> float:
    try:
        if value is None:
            return 0.0
        s = str(value).strip().replace("+", "")
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _safe_date(value: str) -> date | None:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


@dataclass
class LedgerRow:
    timestamp: str
    user_id: str
    user_name: str
    action: str
    current_off: float
    delta: float
    final_off: float
    approved_by: str
    application_date: str
    remarks: str
    holiday_kind: str
    ph_total: float
    expiry: str
    special_total: float


@dataclass
class EntryDetail:
    date: str
    qty: float
    expiry: str
    remarks: str


@dataclass
class UserSummary:
    user_id: str
    user_name: str
    total_balance: float          # available total only
    normal_balance: float
    ph_active: float
    ph_expired: float
    special_active: float
    special_expired: float
    ph_active_entries: List[EntryDetail]
    ph_expired_entries: List[EntryDetail]
    special_active_entries: List[EntryDetail]
    special_expired_entries: List[EntryDetail]
    last_action: str
    last_application_date: str


def _parse_rows(get_all_rows_fn) -> List[LedgerRow]:
    rows = get_all_rows_fn()
    if not rows:
        return []

    parsed: List[LedgerRow] = []
    for r in rows[1:]:
        if len(r) < 14:
            r = r + [""] * (14 - len(r))

        parsed.append(
            LedgerRow(
                timestamp=r[0].strip(),
                user_id=r[1].strip(),
                user_name=r[2].strip(),
                action=r[3].strip(),
                current_off=_safe_float(r[4]),
                delta=_safe_float(r[5]),
                final_off=_safe_float(r[6]),
                approved_by=r[7].strip(),
                application_date=r[8].strip(),
                remarks=r[9].strip(),
                holiday_kind=r[10].strip(),
                ph_total=_safe_float(r[11]),
                expiry=r[12].strip(),
                special_total=_safe_float(r[13]),
            )
        )
    return parsed


def _allocate_remaining(grants: List[dict], claims: List[float]) -> None:
    for claim_qty in claims:
        left = float(claim_qty)
        for grant in grants:
            if left <= 0:
                break
            take = min(grant["remaining"], left)
            grant["remaining"] -= take
            left -= take


def _is_ph_row(r: LedgerRow) -> bool:
    return r.holiday_kind.strip().lower() in ("yes", "y", "true", "1")


def _is_special_row(r: LedgerRow) -> bool:
    return r.holiday_kind.strip().lower() == "special"


def _is_normal_row(r: LedgerRow) -> bool:
    return not _is_ph_row(r) and not _is_special_row(r)


def _build_bucket_breakdown(rows: List[LedgerRow]) -> tuple[float, float, List[EntryDetail], List[EntryDetail]]:
    today = date.today()
    grants = []
    claims = []

    for r in rows:
        qty = r.delta
        if qty > 0:
            grants.append(
                {
                    "remaining": float(qty),
                    "date": r.application_date,
                    "expiry": r.expiry,
                    "remarks": r.remarks,
                }
            )
        elif qty < 0:
            claims.append(abs(float(qty)))

    _allocate_remaining(grants, claims)

    active_total = 0.0
    expired_total = 0.0
    active_entries: List[EntryDetail] = []
    expired_entries: List[EntryDetail] = []

    for g in grants:
        rem = float(g["remaining"])
        if rem <= 0:
            continue

        exp = _safe_date(g["expiry"])
        entry = EntryDetail(
            date=g["date"],
            qty=rem,
            expiry=g["expiry"],
            remarks=g["remarks"],
        )

        if exp and exp < today:
            expired_total += rem
            expired_entries.append(entry)
        else:
            active_total += rem
            active_entries.append(entry)

    return active_total, expired_total, active_entries, expired_entries


def compute_user_summary(user_id: str, get_all_rows_fn) -> UserSummary:
    rows = [r for r in _parse_rows(get_all_rows_fn) if r.user_id == str(user_id)]

    if not rows:
        return UserSummary(
            user_id=str(user_id),
            user_name="Unknown",
            total_balance=0.0,
            normal_balance=0.0,
            ph_active=0.0,
            ph_expired=0.0,
            special_active=0.0,
            special_expired=0.0,
            ph_active_entries=[],
            ph_expired_entries=[],
            special_active_entries=[],
            special_expired_entries=[],
            last_action="",
            last_application_date="",
        )

    last = rows[-1]

    normal_balance = sum(r.delta for r in rows if _is_normal_row(r))

    ph_rows = [r for r in rows if _is_ph_row(r)]
    ph_active, ph_expired, ph_active_entries, ph_expired_entries = _build_bucket_breakdown(ph_rows)

    special_rows = [r for r in rows if _is_special_row(r)]
    special_active, special_expired, special_active_entries, special_expired_entries = _build_bucket_breakdown(special_rows)

    total_balance = normal_balance + ph_active + special_active

    return UserSummary(
        user_id=last.user_id,
        user_name=last.user_name or "Unknown",
        total_balance=total_balance,
        normal_balance=normal_balance,
        ph_active=ph_active,
        ph_expired=ph_expired,
        special_active=special_active,
        special_expired=special_expired,
        ph_active_entries=ph_active_entries,
        ph_expired_entries=ph_expired_entries,
        special_active_entries=special_active_entries,
        special_expired_entries=special_expired_entries,
        last_action=last.action,
        last_application_date=last.application_date,
    )


def compute_overview(get_all_rows_fn) -> List[UserSummary]:
    rows = _parse_rows(get_all_rows_fn)
    seen = set()
    user_ids = []

    for r in rows:
        if not r.user_id or r.user_id in seen:
            continue
        seen.add(r.user_id)
        user_ids.append(r.user_id)

    summaries = [compute_user_summary(uid, get_all_rows_fn) for uid in user_ids]
    summaries.sort(key=lambda x: x.user_name.lower())
    return summaries

def get_user_last_records(user_id: str, get_all_rows_fn, limit: int = 5) -> List[LedgerRow]:
    rows = [r for r in _parse_rows(get_all_rows_fn) if r.user_id == str(user_id)]
    return rows[-limit:]
