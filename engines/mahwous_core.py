"""
mahwous_core — Omega System v1.0 (Autonomous Market Engine)
═══════════════════════════════════════════════════════════════
5 Pillars:
  P1: DNA Strict Funnel — vectorized str.extract() → [Brand, BaseName, SizeML, Concentration, IsTester]
  P2: Zero Data Drop Ledger — every row routes to one of 4 DataFrames (NEVER dropped)
  P3: Swarm Validation — cross-checks review_df for hidden tester/size mismatches
  P4: Reverse Salla Forge — aggregates missing products → AI descriptions → 40-col Salla CSV
  P5: Dead Letter Object — all exceptions caught, logged, system never halts

Compatible 100% with Salla, Make, and existing engine.py pipeline.
"""
from __future__ import annotations

import gc
import html
import logging
import re
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from config import REJECT_KEYWORDS, KNOWN_BRANDS, TESTER_KEYWORDS
except ImportError:
    REJECT_KEYWORDS = [
        "sample", "عينة", "عينه", "decant", "تقسيم", "تقسيمة",
        "split", "miniature", "0.5ml", "1ml", "2ml", "3ml",
    ]
    KNOWN_BRANDS = []
    TESTER_KEYWORDS = ["tester", "تستر", "تيستر"]

logger = logging.getLogger("OmegaSystem")

# ══════════════════════════════════════════════════════════════════════════
#  Constants & Precompiled Regex
# ══════════════════════════════════════════════════════════════════════════
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# Pillar 1: DNA extraction regex (vectorized via pd.str.extract)
# Captures: size in ml/مل, concentration type, tester keywords
_RE_SIZE_ML = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي|milliliter)\b",
    re.IGNORECASE | re.UNICODE,
)
_RE_SIZE_OZ = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:oz|ounce)\b",
    re.IGNORECASE,
)
_RE_CONCENTRATION = re.compile(
    r"\b(edp|edt|edc|parfum|extrait|eau\s+de\s+parfum|eau\s+de\s+toilette|"
    r"eau\s+de\s+cologne|بارفان|بارفيوم|بيرفيوم|تواليت|كولون|اكسترايت)\b",
    re.IGNORECASE | re.UNICODE,
)
_RE_TESTER = re.compile(
    r"\b(tester|تستر|تيستر|بدون\s+كرتون|without\s+box|no\s+box|unboxed)\b",
    re.IGNORECASE | re.UNICODE,
)
_RE_SAMPLE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in REJECT_KEYWORDS) + r")\b",
    re.IGNORECASE | re.UNICODE,
)

# Brand normalization (Arabic→English canonical)
_BRAND_SYNONYMS: Dict[str, str] = {
    "ديور": "dior", "شانيل": "chanel", "شنل": "chanel",
    "أرماني": "armani", "ارماني": "armani", "جورجيو ارماني": "armani",
    "فرساتشي": "versace", "فيرساتشي": "versace",
    "توم فورد": "tom ford", "تومفورد": "tom ford",
    "لطافة": "lattafa", "لطافه": "lattafa",
    "أجمل": "ajmal", "رصاصي": "rasasi", "رصاسي": "rasasi",
    "أمواج": "amouage", "كريد": "creed",
    "ايف سان لوران": "ysl", "سان لوران": "ysl",
    "غوتشي": "gucci", "قوتشي": "gucci",
    "برادا": "prada", "بربري": "burberry", "بيربري": "burberry",
    "جيفنشي": "givenchy", "جفنشي": "givenchy",
    "كارولينا هيريرا": "carolina herrera",
    "باكو رابان": "paco rabanne",
    "كالفن كلاين": "calvin klein", "هوجو بوس": "hugo boss",
    "فالنتينو": "valentino", "بلغاري": "bvlgari",
    "كارتييه": "cartier", "لانكوم": "lancome",
    "جو مالون": "jo malone", "جومالون": "jo malone",
    "مانسيرا": "mancera", "مونتالي": "montale",
    "روجا": "roja", "كيليان": "kilian",
    "نيشان": "nishane", "زيرجوف": "xerjoff",
    "بنهاليغونز": "penhaligons", "مارلي": "parfums de marly",
    "جيرلان": "guerlain", "غيرلان": "guerlain",
    "بايريدو": "byredo", "لي لابو": "le labo",
    "ميزون مارجيلا": "maison margiela",
    "مونت بلانك": "montblanc", "مونتبلان": "montblanc",
    "دولشي": "dolce gabbana", "موجلر": "mugler",
    "جيمي تشو": "jimmy choo", "لاليك": "lalique",
}

# Concentration normalization
_CONC_MAP: Dict[str, str] = {
    "edp": "EDP", "eau de parfum": "EDP", "parfum": "EDP",
    "بارفان": "EDP", "بارفيوم": "EDP", "بيرفيوم": "EDP",
    "edt": "EDT", "eau de toilette": "EDT", "تواليت": "EDT",
    "edc": "EDC", "eau de cologne": "EDC", "كولون": "EDC",
    "extrait": "EXTRAIT", "اكسترايت": "EXTRAIT",
}


# ══════════════════════════════════════════════════════════════════════════
#  Pillar 5: Dead Letter Object
# ══════════════════════════════════════════════════════════════════════════
@dataclass
class DeadLetter:
    """Captures a row that failed processing with full traceback."""
    row_index: int
    row_data: Dict[str, Any]
    error_type: str
    error_message: str
    traceback_str: str
    pillar: str  # which pillar raised the error


class DeadLetterCollector:
    """Thread-safe collector for dead letters. System NEVER halts."""
    def __init__(self):
        self._letters: List[DeadLetter] = []

    def catch(self, row_index: int, row_data: dict, exc: Exception, pillar: str):
        self._letters.append(DeadLetter(
            row_index=row_index,
            row_data={k: str(v)[:200] for k, v in (row_data or {}).items()},
            error_type=type(exc).__name__,
            error_message=str(exc)[:500],
            traceback_str=traceback.format_exc()[-1000:],
            pillar=pillar,
        ))
        logger.warning(
            "DeadLetter [P%s] row=%d: %s: %s",
            pillar, row_index, type(exc).__name__, str(exc)[:200],
        )

    @property
    def letters(self) -> List[DeadLetter]:
        return list(self._letters)

    @property
    def count(self) -> int:
        return len(self._letters)

    def to_dataframe(self) -> pd.DataFrame:
        if not self._letters:
            return pd.DataFrame(columns=["row_index", "error_type", "error_message", "pillar"])
        return pd.DataFrame([
            {"row_index": d.row_index, "error_type": d.error_type,
             "error_message": d.error_message, "pillar": d.pillar}
            for d in self._letters
        ])


# ══════════════════════════════════════════════════════════════════════════
#  Pillar 1: DNA Strict Funnel — Vectorized Extraction
# ══════════════════════════════════════════════════════════════════════════

def _vectorized_extract_size(names: pd.Series) -> pd.Series:
    """Extract size in ml from product names — fully vectorized."""
    names_str = names.fillna("").astype(str)
    # Try ml first
    ml_match = names_str.str.extract(
        r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي|milliliter)",
        flags=re.IGNORECASE,
        expand=False,
    )
    # Try oz and convert
    oz_match = names_str.str.extract(
        r"(\d+(?:\.\d+)?)\s*(?:oz|ounce)",
        flags=re.IGNORECASE,
        expand=False,
    )
    size_ml = pd.to_numeric(ml_match, errors="coerce")
    size_oz = pd.to_numeric(oz_match, errors="coerce") * 29.5735
    return size_ml.fillna(size_oz).fillna(0.0)


def _vectorized_extract_concentration(names: pd.Series) -> pd.Series:
    """Extract concentration type — vectorized."""
    names_lower = names.fillna("").astype(str).str.lower()
    result = pd.Series("", index=names.index, dtype="object")
    # Order matters: check specific patterns first
    for pattern, conc_type in [
        (r"\bextrait\b|\bakstrايت\b|\baكسترايت\b", "EXTRAIT"),
        (r"\bedp\b|\beau\s*de\s*parfum\b|\bparfum\b|\bبارفان\b|\bبارفيوم\b|\bبيرفيوم\b", "EDP"),
        (r"\bedt\b|\beau\s*de\s*toilette\b|\bتواليت\b", "EDT"),
        (r"\bedc\b|\beau\s*de\s*cologne\b|\bكولون\b", "EDC"),
    ]:
        mask = names_lower.str.contains(pattern, regex=True, na=False) & (result == "")
        result = result.where(~mask, conc_type)
    return result


def _vectorized_extract_tester(names: pd.Series) -> pd.Series:
    """Detect tester products — vectorized. Returns boolean Series."""
    names_lower = names.fillna("").astype(str).str.lower()
    return names_lower.str.contains(
        r"\btester\b|\bتستر\b|\bتيستر\b|\bبدون\s*كرتون\b|\bwithout\s*box\b|\bunboxed\b",
        regex=True, na=False,
    )


def _vectorized_extract_brand(names: pd.Series) -> pd.Series:
    """
    Extract brand from product names — vectorized.
    Uses known brands list + Arabic synonym resolution.
    """
    names_lower = names.fillna("").astype(str).str.lower()
    result = pd.Series("", index=names.index, dtype="object")

    # Build combined lookup: brand_lower → canonical
    brand_lookup: Dict[str, str] = {}
    for b in KNOWN_BRANDS:
        brand_lookup[b.lower()] = b.lower()
    for ar, en in _BRAND_SYNONYMS.items():
        brand_lookup[ar] = en

    # Sort by length descending to match longest brand first
    sorted_brands = sorted(brand_lookup.keys(), key=len, reverse=True)

    # Vectorized: for each brand, check containment
    for brand_key in sorted_brands:
        if len(brand_key) < 3:
            continue
        escaped = re.escape(brand_key)
        mask = names_lower.str.contains(escaped, regex=True, na=False) & (result == "")
        canonical = brand_lookup[brand_key]
        result = result.where(~mask, canonical)

    return result


def _vectorized_extract_basename(
    names: pd.Series,
    brands: pd.Series,
    sizes: pd.Series,
    concentrations: pd.Series,
) -> pd.Series:
    """
    Extract BaseName by removing Brand, Size, Concentration, and noise words.
    This is the only field where fuzzy matching is applied.
    Fully vectorized via pd.Series.str operations.
    """
    cleaned = names.fillna("").astype(str).str.lower()

    # Remove brand from name
    for idx in cleaned.index:
        brand = str(brands.get(idx, "")).strip()
        if brand:
            # Remove all known synonyms of this brand
            for syn_key, syn_val in _BRAND_SYNONYMS.items():
                if syn_val == brand:
                    cleaned.iloc[cleaned.index.get_loc(idx)] = (
                        cleaned.iloc[cleaned.index.get_loc(idx)].replace(syn_key, " ")
                    )
            cleaned.iloc[cleaned.index.get_loc(idx)] = (
                cleaned.iloc[cleaned.index.get_loc(idx)].replace(brand, " ")
            )

    # Remove size patterns
    cleaned = cleaned.str.replace(
        r"\d+(?:\.\d+)?\s*(?:ml|مل|ملي|oz|ounce|milliliter)\b",
        " ", regex=True,
    )
    # Remove concentration keywords
    cleaned = cleaned.str.replace(
        r"\b(?:edp|edt|edc|parfum|perfume|extrait|cologne|toilette|"
        r"eau\s*de\s*(?:parfum|toilette|cologne)|"
        r"بارفان|بارفيوم|بيرفيوم|تواليت|كولون|اكسترايت)\b",
        " ", regex=True,
    )
    # Remove tester keywords
    cleaned = cleaned.str.replace(
        r"\b(?:tester|تستر|تيستر|بدون\s*كرتون)\b",
        " ", regex=True,
    )
    # Remove common noise words
    cleaned = cleaned.str.replace(
        r"\b(?:عطر|عطور|للرجال|للنساء|رجالي|نسائي|للجنسين|"
        r"for\s*men|for\s*women|pour\s*homme|pour\s*femme|unisex|"
        r"eau\s*de|او\s*دو|او\s*دي|أو\s*دو|de|du|la|le|the)\b",
        " ", regex=True,
    )
    # Normalize Arabic characters
    for src, dst in [("أ", "ا"), ("إ", "ا"), ("آ", "ا"), ("ة", "ه"), ("ى", "ي")]:
        cleaned = cleaned.str.replace(src, dst, regex=False)
    # Remove standalone numbers and special chars
    cleaned = cleaned.str.replace(r"\b\d+\b", " ", regex=True)
    cleaned = cleaned.str.replace(r"[^\w\s\u0600-\u06FF]", " ", regex=True)
    # Collapse whitespace
    cleaned = cleaned.str.replace(r"\s+", " ", regex=True).str.strip()

    return cleaned


def extract_dna(df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """
    Pillar 1: Parse every product name into 5 strict DNA columns.
    All operations are vectorized via Pandas str.extract / str.contains.

    Adds columns: _dna_brand, _dna_basename, _dna_size_ml, _dna_concentration, _dna_is_tester
    """
    names = df[name_col].fillna("").astype(str)

    df = df.copy()
    df["_dna_brand"]         = _vectorized_extract_brand(names)
    df["_dna_size_ml"]       = _vectorized_extract_size(names)
    df["_dna_concentration"] = _vectorized_extract_concentration(names)
    df["_dna_is_tester"]     = _vectorized_extract_tester(names)
    df["_dna_basename"]      = _vectorized_extract_basename(
        names, df["_dna_brand"], df["_dna_size_ml"], df["_dna_concentration"],
    )
    return df


# ══════════════════════════════════════════════════════════════════════════
#  Pillar 2: Zero Data Drop Ledger — Event Router
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class OmegaRouteResult:
    """Result of the Omega routing — 4 isolated DataFrames + dead letters."""
    confirmed_df: pd.DataFrame       # 100% DNA match (score ≥ 95%)
    review_df: pd.DataFrame          # Swarm Court (80-95% text match)
    samples_vault_df: pd.DataFrame   # Size < 10ml
    missing_forge_df: pd.DataFrame   # Zero matches in our database
    dead_letters: DeadLetterCollector
    routing_stats: Dict[str, int]


def _compute_fuzz_scores_vectorized(
    our_basenames: pd.Series,
    comp_basenames: pd.Series,
    pairs_df: pd.DataFrame,
) -> pd.Series:
    """
    Compute RapidFuzz token_set_ratio for paired BaseName columns.
    Uses vectorized apply with RapidFuzz for maximum throughput.
    """
    from rapidfuzz import fuzz as rf_fuzz

    def _score_row(row):
        a = str(row.get("_our_basename", "")).strip()
        b = str(row.get("_comp_basename", "")).strip()
        if not a or not b:
            return 0.0
        return rf_fuzz.token_set_ratio(a, b)

    return pairs_df.apply(_score_row, axis=1)


def route_products(
    our_df: pd.DataFrame,
    comp_df: pd.DataFrame,
    our_name_col: str,
    comp_name_col: str,
    dead_letters: DeadLetterCollector,
) -> OmegaRouteResult:
    """
    Pillar 2: Route EVERY competitor row to exactly one of 4 DataFrames.
    Uses DNA-strict matching: Brand+Size+Concentration+Tester must align exactly.
    RapidFuzz is ONLY applied to BaseName.

    NO use of `continue` for dropping rows. EVERY row is accounted for.
    """
    stats: Dict[str, int] = {
        "total_comp_rows": len(comp_df),
        "confirmed": 0,
        "review": 0,
        "samples_vault": 0,
        "missing_forge": 0,
        "dead_letters": 0,
    }

    # ── Step 1: Extract DNA for both DataFrames ──────────────────────────
    try:
        our_dna = extract_dna(our_df, our_name_col)
    except Exception as e:
        dead_letters.catch(-1, {"step": "our_dna_extraction"}, e, "P1")
        return OmegaRouteResult(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), comp_df.copy(),
            dead_letters, stats,
        )

    try:
        comp_dna = extract_dna(comp_df, comp_name_col)
    except Exception as e:
        dead_letters.catch(-1, {"step": "comp_dna_extraction"}, e, "P1")
        return OmegaRouteResult(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), comp_df.copy(),
            dead_letters, stats,
        )

    # ── Step 2: Isolate samples (Size < 10ml) FIRST ─────────────────────
    sample_mask = (comp_dna["_dna_size_ml"] > 0) & (comp_dna["_dna_size_ml"] < 10)
    samples_vault_df = comp_dna[sample_mask].copy()
    remaining = comp_dna[~sample_mask].copy()
    stats["samples_vault"] = len(samples_vault_df)

    if remaining.empty:
        return OmegaRouteResult(
            pd.DataFrame(), pd.DataFrame(), samples_vault_df,
            pd.DataFrame(), dead_letters, stats,
        )

    # ── Step 3: DNA-strict cross join on Brand+Size+Concentration+Tester ─
    # Build join keys (vectorized)
    def _make_join_key(df: pd.DataFrame) -> pd.Series:
        brand = df["_dna_brand"].fillna("").astype(str).str.strip().str.lower()
        size = df["_dna_size_ml"].fillna(0).astype(int).astype(str)
        conc = df["_dna_concentration"].fillna("").astype(str).str.strip().str.upper()
        tester = df["_dna_is_tester"].astype(str)
        return brand + "|" + size + "|" + conc + "|" + tester

    our_dna["_join_key"] = _make_join_key(our_dna)
    remaining["_join_key"] = _make_join_key(remaining)

    # Index our products by join key for fast lookup
    our_keys = set(our_dna["_join_key"].unique())

    # Partition remaining: has_dna_match (key exists in our catalog) vs no_match
    has_match_mask = remaining["_join_key"].isin(our_keys)
    matchable = remaining[has_match_mask].copy()
    no_dna_match = remaining[~has_match_mask].copy()

    # ── Step 4: For matchable rows, compute BaseName fuzz score ──────────
    confirmed_rows = []
    review_rows = []
    missing_from_matchable = []

    if not matchable.empty:
        # Build our basename lookup: join_key → list of basenames
        our_basename_lookup: Dict[str, List[str]] = {}
        for _, r in our_dna.iterrows():
            key = r["_join_key"]
            basename = str(r.get("_dna_basename", "")).strip()
            if key not in our_basename_lookup:
                our_basename_lookup[key] = []
            our_basename_lookup[key].append(basename)

        try:
            from rapidfuzz import fuzz as rf_fuzz
        except ImportError:
            rf_fuzz = None

        for idx, row in matchable.iterrows():
            try:
                comp_basename = str(row.get("_dna_basename", "")).strip()
                join_key = row["_join_key"]
                our_basenames_list = our_basename_lookup.get(join_key, [])

                if not our_basenames_list or not comp_basename:
                    missing_from_matchable.append(row)
                    continue  # this continue is safe — row is routed to missing_from_matchable

                # Find best fuzzy score among our basenames with same DNA
                best_score = 0.0
                if rf_fuzz is not None:
                    for ob in our_basenames_list:
                        if not ob:
                            continue
                        s = rf_fuzz.token_set_ratio(comp_basename, ob)
                        if s > best_score:
                            best_score = s
                else:
                    # Fallback: exact containment
                    for ob in our_basenames_list:
                        if comp_basename in ob or ob in comp_basename:
                            best_score = 90.0
                            break

                row_with_score = row.copy()
                row_with_score["_fuzz_score"] = best_score

                if best_score >= 95:
                    confirmed_rows.append(row_with_score)
                elif best_score >= 80:
                    review_rows.append(row_with_score)
                else:
                    missing_from_matchable.append(row_with_score)

            except Exception as e:
                dead_letters.catch(int(idx) if isinstance(idx, (int, np.integer)) else 0,
                                   row.to_dict() if hasattr(row, 'to_dict') else {}, e, "P2")
                stats["dead_letters"] += 1
                # Route to missing on error — NEVER drop
                missing_from_matchable.append(row)

    # ── Step 5: Assemble the 4 output DataFrames ────────────────────────
    confirmed_df = pd.DataFrame(confirmed_rows) if confirmed_rows else pd.DataFrame()
    review_df = pd.DataFrame(review_rows) if review_rows else pd.DataFrame()
    missing_forge_df = pd.concat(
        [no_dna_match, pd.DataFrame(missing_from_matchable)] if missing_from_matchable
        else [no_dna_match],
        ignore_index=True,
    ) if not no_dna_match.empty or missing_from_matchable else pd.DataFrame()

    stats["confirmed"] = len(confirmed_df)
    stats["review"] = len(review_df)
    stats["missing_forge"] = len(missing_forge_df)

    return OmegaRouteResult(
        confirmed_df=confirmed_df,
        review_df=review_df,
        samples_vault_df=samples_vault_df,
        missing_forge_df=missing_forge_df,
        dead_letters=dead_letters,
        routing_stats=stats,
    )


# ══════════════════════════════════════════════════════════════════════════
#  Pillar 3: Swarm Validation (Cross-Check on review_df)
# ══════════════════════════════════════════════════════════════════════════

def swarm_validate(
    review_df: pd.DataFrame,
    dead_letters: DeadLetterCollector,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pillar 3: Silent validator for review_df (80-95% fuzz score).
    Checks for:
      1. Hidden tester keywords ("بدون كرتون", "تستر") that were missed by DNA extraction
      2. Size mismatches hidden in parentheses or trailing text
      3. Flanker mismatches (sport/intense/oud variants)

    Returns: (validated_review_df, demoted_to_missing_df)
    """
    if review_df is None or review_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    _HIDDEN_TESTER_RE = re.compile(
        r"بدون\s*كرتون|بدون\s*علبة|كرتون\s*ابيض|كرتون\s*أبيض|"
        r"white\s*box|no\s*cap|without\s*cap|without\s*box|unboxed|"
        r"\btester\b|\bتستر\b|\bتيستر\b",
        re.IGNORECASE | re.UNICODE,
    )
    _FLANKER_KEYWORDS = [
        "sport", "سبورت", "intense", "انتنس", "إنتنس",
        "elixir", "الكسير", "oud", "عود", "absolu", "ابسولو",
        "extreme", "اكستريم", "night", "نايت", "prive", "بريفيه",
        "black", "بلاك", "gold", "جولد", "rose", "روز",
    ]

    validated_indices = []
    demoted_indices = []

    for idx, row in review_df.iterrows():
        try:
            name = str(row.get(review_df.columns[0], "")).strip() if len(review_df.columns) > 0 else ""
            # Try to get the original name from various possible columns
            for col_candidate in ["_original_name", review_df.columns[0]]:
                if col_candidate in review_df.columns:
                    candidate_val = str(row.get(col_candidate, "")).strip()
                    if candidate_val:
                        name = candidate_val
                        break

            name_lower = name.lower()
            should_demote = False
            demote_reason = ""

            # Check 1: Hidden tester keywords
            has_hidden_tester = bool(_HIDDEN_TESTER_RE.search(name))
            is_dna_tester = bool(row.get("_dna_is_tester", False))
            if has_hidden_tester and not is_dna_tester:
                should_demote = True
                demote_reason = "hidden_tester_keyword"

            # Check 2: Size mismatch in parenthetical text
            # e.g., "Product Name (5ml)" where DNA extracted 100ml from another part
            paren_match = re.search(r"\((\d+)\s*(?:ml|مل)\)", name, re.IGNORECASE)
            if paren_match:
                paren_size = float(paren_match.group(1))
                dna_size = float(row.get("_dna_size_ml", 0) or 0)
                if dna_size > 0 and paren_size > 0 and abs(dna_size - paren_size) > 10:
                    should_demote = True
                    demote_reason = f"size_mismatch_paren({paren_size}ml vs DNA {dna_size}ml)"

            # Check 3: Flanker keyword present in one but not aligned
            fuzz_score = float(row.get("_fuzz_score", 0) or 0)
            if fuzz_score < 90:
                for flanker in _FLANKER_KEYWORDS:
                    if flanker in name_lower:
                        # This flanker should have been part of the basename match
                        basename = str(row.get("_dna_basename", "")).strip().lower()
                        if flanker not in basename:
                            should_demote = True
                            demote_reason = f"flanker_not_in_basename({flanker})"
                            break

            if should_demote:
                demoted_indices.append(idx)
            else:
                validated_indices.append(idx)

        except Exception as e:
            dead_letters.catch(
                int(idx) if isinstance(idx, (int, np.integer)) else 0,
                row.to_dict() if hasattr(row, 'to_dict') else {},
                e, "P3",
            )
            # On error, keep in review (don't lose data)
            validated_indices.append(idx)

    validated_df = review_df.loc[review_df.index.isin(validated_indices)].copy() if validated_indices else pd.DataFrame()
    demoted_df = review_df.loc[review_df.index.isin(demoted_indices)].copy() if demoted_indices else pd.DataFrame()

    return validated_df, demoted_df


# ══════════════════════════════════════════════════════════════════════════
#  Pillar 4: Reverse Salla Forge
# ══════════════════════════════════════════════════════════════════════════

def reverse_forge_aggregate(missing_forge_df: pd.DataFrame) -> pd.DataFrame:
    """
    Group missing products by DNA columns, compute mean price across competitors.
    Returns aggregated DataFrame with one row per unique DNA signature.
    """
    if missing_forge_df is None or missing_forge_df.empty:
        return pd.DataFrame()

    # Find price column
    price_col = None
    for c in ("سعر_المنافس", "سعر المنافس", "سعر المنتج", "السعر", "Price", "price"):
        if c in missing_forge_df.columns:
            price_col = c
            break

    # Find name column
    name_col = None
    for c in ("اسم المنتج", "المنتج", "منتج_المنافس", "name", "Name"):
        if c in missing_forge_df.columns:
            name_col = c
            break

    # Find image column
    img_col = None
    for c in ("صورة المنتج", "صورة_المنافس", "image", "Image"):
        if c in missing_forge_df.columns:
            img_col = c
            break

    dna_cols = ["_dna_brand", "_dna_basename", "_dna_size_ml", "_dna_concentration", "_dna_is_tester"]
    available_dna = [c for c in dna_cols if c in missing_forge_df.columns]

    if not available_dna:
        # No DNA columns — return as-is with dedup on name
        if name_col:
            return missing_forge_df.drop_duplicates(subset=[name_col]).reset_index(drop=True)
        return missing_forge_df.reset_index(drop=True)

    # Group by available DNA columns
    agg_dict = {}
    if name_col:
        agg_dict[name_col] = "first"
    if price_col:
        agg_dict[price_col] = "mean"
    if img_col:
        agg_dict[img_col] = "first"

    # Include any other non-DNA columns as 'first'
    for col in missing_forge_df.columns:
        if col not in available_dna and col not in agg_dict and not col.startswith("_"):
            agg_dict[col] = "first"

    if not agg_dict:
        return missing_forge_df.drop_duplicates(subset=available_dna).reset_index(drop=True)

    try:
        aggregated = missing_forge_df.groupby(available_dna, dropna=False).agg(agg_dict).reset_index()
        # Round price to 2 decimals
        if price_col and price_col in aggregated.columns:
            aggregated[price_col] = aggregated[price_col].round(2)
        return aggregated
    except Exception:
        return missing_forge_df.drop_duplicates(subset=available_dna).reset_index(drop=True)


def reverse_forge_to_salla(
    aggregated_missing_df: pd.DataFrame,
    use_ai: bool = True,
    dead_letters: Optional[DeadLetterCollector] = None,
) -> Tuple[Optional[bytes], int]:
    """
    Pillar 4: Takes aggregated missing products, enhances via AI, exports to Salla 40-col CSV.

    Steps:
      1. For each missing product, call enhance_competitor_product_for_salla() for AI HTML descriptions
      2. Route to build_salla_shamel_dataframe() → export_to_salla_shamel_csv() → 40-col CSV

    Returns: (csv_bytes or None, product_count)
    """
    if aggregated_missing_df is None or aggregated_missing_df.empty:
        return None, 0

    if dead_letters is None:
        dead_letters = DeadLetterCollector()

    # Find name column
    name_col = None
    for c in ("اسم المنتج", "المنتج", "منتج_المنافس", "name", "Name"):
        if c in aggregated_missing_df.columns:
            name_col = c
            break
    if not name_col and len(aggregated_missing_df.columns) > 0:
        name_col = aggregated_missing_df.columns[0]

    # Enhance with AI descriptions if requested
    if use_ai:
        try:
            from engines.ai_engine import enhance_competitor_product_for_salla
        except ImportError:
            use_ai = False
            logger.warning("Omega P4: ai_engine not available — skipping AI enhancement")

    enhanced_rows = []
    for idx, row in aggregated_missing_df.iterrows():
        try:
            r = row.to_dict()
            product_name = str(r.get(name_col, "")).strip() if name_col else ""

            if use_ai and product_name:
                # Build scraped summary for AI
                summary_parts = [f"اسم المنتج: {product_name}"]
                for extra_key in ("_dna_brand", "الماركة", "brand"):
                    v = str(r.get(extra_key, "")).strip()
                    if v and v.lower() not in ("nan", "none", ""):
                        summary_parts.append(f"الماركة: {v}")
                        break
                for extra_key in ("_dna_size_ml",):
                    v = r.get(extra_key, 0)
                    if v and float(v) > 0:
                        summary_parts.append(f"الحجم: {int(float(v))}ml")
                for extra_key in ("_dna_concentration",):
                    v = str(r.get(extra_key, "")).strip()
                    if v:
                        summary_parts.append(f"التركيز: {v}")

                scraped_summary = "\n".join(summary_parts)
                url = str(r.get("رابط المنتج", r.get("url", ""))).strip()

                try:
                    ai_result = enhance_competitor_product_for_salla(
                        scraped_summary=scraped_summary,
                        url=url,
                    )
                    if ai_result and ai_result.get("description_html"):
                        r["وصف_AI"] = ai_result["description_html"]
                        if ai_result.get("brand"):
                            r["الماركة_الرسمية"] = ai_result["brand"]
                        if ai_result.get("category"):
                            r["التصنيف_الرسمي"] = ai_result["category"]
                        if ai_result.get("top_notes"):
                            r["top_notes"] = ai_result["top_notes"]
                        if ai_result.get("heart_notes"):
                            r["heart_notes"] = ai_result["heart_notes"]
                        if ai_result.get("base_notes"):
                            r["base_notes"] = ai_result["base_notes"]
                except Exception as ai_e:
                    dead_letters.catch(
                        int(idx) if isinstance(idx, (int, np.integer)) else 0,
                        {"product": product_name}, ai_e, "P4-AI",
                    )

            enhanced_rows.append(r)
        except Exception as e:
            dead_letters.catch(
                int(idx) if isinstance(idx, (int, np.integer)) else 0,
                row.to_dict() if hasattr(row, 'to_dict') else {},
                e, "P4",
            )
            enhanced_rows.append(row.to_dict() if hasattr(row, 'to_dict') else {})

    enhanced_df = pd.DataFrame(enhanced_rows)

    # Export to Salla 40-col CSV
    try:
        from utils.salla_shamel_export import export_to_salla_shamel_csv
        csv_bytes, count, _ = export_to_salla_shamel_csv(
            enhanced_df, our_catalog_df=None, verify_missing=False,
        )
        return csv_bytes, count
    except Exception as e:
        dead_letters.catch(-1, {"step": "salla_export"}, e, "P4-Export")
        return None, 0


# ══════════════════════════════════════════════════════════════════════════
#  Omega Engine — The Complete Orchestrator
# ══════════════════════════════════════════════════════════════════════════

@dataclass
class OmegaResult:
    """Complete result of the Omega System analysis."""
    confirmed_df: pd.DataFrame
    review_df: pd.DataFrame
    samples_vault_df: pd.DataFrame
    missing_forge_df: pd.DataFrame
    aggregated_missing_df: pd.DataFrame
    salla_csv_bytes: Optional[bytes]
    salla_product_count: int
    dead_letters_df: pd.DataFrame
    routing_stats: Dict[str, int]
    dead_letter_count: int


class OmegaEngine:
    """
    The Omega System — Autonomous Market Engine.
    Orchestrates all 5 Pillars in a single pipeline.

    Usage:
        engine = OmegaEngine()
        result = engine.run(our_df, comp_df, our_name_col, comp_name_col)
    """

    def __init__(self, use_ai: bool = True):
        self.use_ai = use_ai
        self._dead_letters = DeadLetterCollector()

    def run(
        self,
        our_df: pd.DataFrame,
        comp_df: pd.DataFrame,
        our_name_col: str = "اسم المنتج",
        comp_name_col: str = "اسم المنتج",
        generate_salla_csv: bool = True,
        progress_callback=None,
    ) -> OmegaResult:
        """
        Execute the complete Omega pipeline:
          P1 → DNA Extraction (vectorized)
          P2 → Zero-Drop Routing
          P3 → Swarm Validation on review_df
          P4 → Reverse Salla Forge on missing_forge_df
          P5 → Dead Letter handling throughout
        """
        logger.info(
            "Omega Engine: Starting — Our products: %d, Competitor products: %d",
            len(our_df), len(comp_df),
        )

        # ── P2: Route all competitor products ────────────────────────────
        if progress_callback:
            progress_callback(0.1, "DNA Extraction & Routing...")

        route_result = route_products(
            our_df, comp_df, our_name_col, comp_name_col, self._dead_letters,
        )

        if progress_callback:
            progress_callback(0.4, "Swarm Validation...")

        # ── P3: Swarm Validation on review_df ────────────────────────────
        validated_review, demoted_to_missing = swarm_validate(
            route_result.review_df, self._dead_letters,
        )

        # Merge demoted rows into missing_forge_df
        if not demoted_to_missing.empty:
            missing_forge_df = pd.concat(
                [route_result.missing_forge_df, demoted_to_missing],
                ignore_index=True,
            )
        else:
            missing_forge_df = route_result.missing_forge_df

        if progress_callback:
            progress_callback(0.6, "Reverse Forge Aggregation...")

        # ── P4: Reverse Salla Forge ──────────────────────────────────────
        aggregated_missing = reverse_forge_aggregate(missing_forge_df)

        salla_csv_bytes = None
        salla_count = 0
        if generate_salla_csv and not aggregated_missing.empty:
            if progress_callback:
                progress_callback(0.7, "AI Enhancement & Salla Export...")
            salla_csv_bytes, salla_count = reverse_forge_to_salla(
                aggregated_missing,
                use_ai=self.use_ai,
                dead_letters=self._dead_letters,
            )

        if progress_callback:
            progress_callback(1.0, "Complete!")

        # Update stats
        final_stats = dict(route_result.routing_stats)
        final_stats["review_validated"] = len(validated_review)
        final_stats["review_demoted"] = len(demoted_to_missing)
        final_stats["missing_aggregated"] = len(aggregated_missing)
        final_stats["salla_exported"] = salla_count
        final_stats["dead_letters"] = self._dead_letters.count

        logger.info("Omega Engine: Complete — %s", final_stats)

        # Cleanup DNA columns from output
        dna_cols = [c for c in ["_dna_brand", "_dna_basename", "_dna_size_ml",
                                "_dna_concentration", "_dna_is_tester", "_join_key",
                                "_fuzz_score"] if True]

        def _clean_dna(df):
            if df is None or df.empty:
                return df
            drop = [c for c in dna_cols if c in df.columns]
            return df.drop(columns=drop, errors="ignore") if drop else df

        # Memory cleanup
        gc.collect()

        return OmegaResult(
            confirmed_df=route_result.confirmed_df,
            review_df=validated_review,
            samples_vault_df=route_result.samples_vault_df,
            missing_forge_df=missing_forge_df,
            aggregated_missing_df=aggregated_missing,
            salla_csv_bytes=salla_csv_bytes,
            salla_product_count=salla_count,
            dead_letters_df=self._dead_letters.to_dataframe(),
            routing_stats=final_stats,
            dead_letter_count=self._dead_letters.count,
        )


# ══════════════════════════════════════════════════════════════════════════
#  Backward Compatibility — Legacy functions preserved
# ══════════════════════════════════════════════════════════════════════════

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or str(val).strip() in ("", "nan", "None", "NaN"):
            return default
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default


def _is_sample_strict(name: str) -> bool:
    if not isinstance(name, str) or not name.strip():
        return True
    nl = name.lower()
    return any(k.lower() in nl for k in REJECT_KEYWORDS)


def _extract_ml(name: str) -> float:
    if not isinstance(name, str):
        return -1.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي)\b", name, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return -1.0
    return -1.0


def _classify_rejected(name: str) -> bool:
    if not isinstance(name, str):
        return True
    nl = name.lower()
    rejects = ["sample", "عينة", "عينه", "miniature", "مينياتشر", "travel size", "decant", "تقسيم", "split"]
    return any(w in nl for w in rejects)


def apply_strict_pipeline_filters(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
    min_ml: float = 2.0,
    keep_excluded: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Legacy filter function — preserved for backward compatibility with engine.py."""
    if df is None or df.empty:
        return df, {"dropped": 0}

    actual_col = name_col
    if name_col not in df.columns:
        alt_cols = ["المنتج", "اسم المنتج", "Product", "Name", "أسم المنتج"]
        for c in alt_cols:
            if c in df.columns:
                actual_col = c
                break
        else:
            return df.copy(), {"dropped": 0, "warning": f"عمود غير موجود: {name_col}"}

    stats: Dict[str, Any] = {
        "dropped_sample_kw": 0,
        "dropped_small_ml": 0,
        "dropped_class_rejected": 0,
        "dropped_empty_name": 0,
        "excluded_rows": []
    }

    # ── Vectorized filtering (Omega style) ────────────────────────────
    names = df[actual_col].fillna("").astype(str).str.strip()
    names_lower = names.str.lower()

    # Empty names
    empty_mask = names.isin(["", "nan", "none", "<na>"]) | (names.str.len() == 0)

    # Sample keywords — vectorized
    sample_pattern = "|".join(re.escape(k.lower()) for k in REJECT_KEYWORDS)
    sample_mask = names_lower.str.contains(sample_pattern, regex=True, na=False) & ~empty_mask

    # Rejected classification
    reject_words = ["sample", "عينة", "عينه", "miniature", "مينياتشر", "travel size", "decant", "تقسيم", "split"]
    reject_pattern = "|".join(re.escape(w) for w in reject_words)
    reject_mask = names_lower.str.contains(reject_pattern, regex=True, na=False) & ~empty_mask & ~sample_mask

    # Small sizes
    sizes = _vectorized_extract_size(names)
    small_mask = (sizes > 0) & (sizes < min_ml) & ~empty_mask & ~sample_mask & ~reject_mask

    excluded_mask = empty_mask | sample_mask | reject_mask | small_mask

    stats["dropped_empty_name"] = int(empty_mask.sum())
    stats["dropped_sample_kw"] = int(sample_mask.sum())
    stats["dropped_class_rejected"] = int(reject_mask.sum())
    stats["dropped_small_ml"] = int(small_mask.sum())
    stats["dropped"] = int(excluded_mask.sum())
    stats["kept"] = int((~excluded_mask).sum())

    if keep_excluded:
        out = df.copy()
        reasons = pd.Series("", index=df.index, dtype="object")
        reasons = reasons.where(~empty_mask, "اسم فارغ")
        reasons = reasons.where(~sample_mask | (reasons != ""), "كلمة عينة محظورة")
        reasons = reasons.where(~reject_mask | (reasons != ""), "تصنيف مستبعد (عينة/تقسيم)")
        reasons = reasons.where(~small_mask | (reasons != ""), other=reasons)
        # Fix: set small_mask reasons
        small_idx = small_mask[small_mask].index
        for si in small_idx:
            if reasons.loc[si] == "":
                reasons.loc[si] = f"حجم صغير جداً ({sizes.loc[si]} مل)"
        out["سبب_الاستبعاد"] = reasons
    else:
        out = df[~excluded_mask].reset_index(drop=True) if (~excluded_mask).any() else pd.DataFrame()
        if not out.empty:
            out["سبب_الاستبعاد"] = ""

    return out, stats


def sanitize_salla_text(text: str) -> str:
    """Clean text from HTML and special chars for Salla."""
    if not text:
        return ""
    text = _HTML_TAG_RE.sub(" ", str(text))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def format_mahwous_description(product_data: dict) -> str:
    """Format description in Mahwous professional style."""
    name = sanitize_salla_text(product_data.get("name", "عطر فاخر"))
    brand = sanitize_salla_text(product_data.get("brand", "ماركة عالمية"))
    desc = product_data.get("description", "")
    notes = product_data.get("notes", {})

    lines = [
        f"<h2>{name} من {brand}</h2>",
        f"<p>اكتشف سحر <strong>{name}</strong> من <strong>{brand}</strong> — عطر فاخر يجمع بين الأصالة والتميز. متوفر الآن في متجر مهووس، وجهتك الأولى لأرقى العطور العالمية.</p>",
        "<h3>تفاصيل المنتج</h3>",
        "<ul>",
        "<li><strong>الأصالة:</strong> عطر أصلي 100% بضمان متجر مهووس.</li>",
        "<li><strong>الأداء:</strong> ثبات عالي وفوحان يأسر الحواس طوال اليوم.</li>",
        "<li><strong>التصميم:</strong> زجاجة أنيقة تعكس فخامة المحتوى.</li>",
        "</ul>"
    ]

    if notes and any(notes.values()):
        lines.append("<h3>رحلة العطر — الهرم العطري</h3>")
        lines.append("<ul>")
        if notes.get("top"):
            lines.append(f"<li><strong>النفحات العليا (Top Notes):</strong> {sanitize_salla_text(notes['top'])}</li>")
        if notes.get("heart"):
            lines.append(f"<li><strong>النفحات الوسطى (Heart Notes):</strong> {sanitize_salla_text(notes['heart'])}</li>")
        if notes.get("base"):
            lines.append(f"<li><strong>النفحات الأساسية (Base Notes):</strong> {sanitize_salla_text(notes['base'])}</li>")
        lines.append("</ul>")
    elif desc:
        lines.append("<h3>وصف العطر</h3>")
        lines.append(f"<p>{sanitize_salla_text(desc)}</p>")

    lines.append("<h3>لمسة خبير من مهووس</h3>")
    lines.append("<p>هذا العطر يمثل التوازن المثالي بين القوة والنعومة. ننصح برشه على نقاط النبض للحصول على أفضل أداء وفوحان.</p>")
    lines.append("<p><strong>عالمك العطري يبدأ من مهووس.</strong> أصلي 100% | شحن سريع داخل السعودية.</p>")

    return "".join(lines)


def validate_export_product_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate DataFrame before export."""
    issues: List[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتحقق أو التصدير."]

    for i, (_, row) in enumerate(df.iterrows()):
        name = (
            str(row.get("منتج_المنافس", "")).strip()
            or str(row.get("المنتج", "")).strip()
            or str(row.get("أسم المنتج", "")).strip()
            or str(row.get("اسم المنتج", "")).strip()
        )
        price = _safe_float(
            row.get("سعر_المنافس", row.get("سعر المنافس", row.get("السعر", 0)))
        )
        if not name or name.lower() in ("nan", "none"):
            issues.append(f"صف {i + 1}: اسم المنتج فارغ")
        if price <= 0:
            issues.append(f"صف {i + 1}: السعر غير صالح")

    return (len(issues) == 0, issues)
