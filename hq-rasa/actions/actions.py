"""
HealthQueue+ Rasa Custom Actions — v4
Rewritten against the REAL hq-server endpoint contracts (queueRoutes.js,
appointmentRoutes.js, clinicRoutes.js, chatbotRoutes.js) — not assumed ones.

Architecture (confirmed from server-src / mobile-lib):
  mobile app --POST /api/chatbot/message--> hq-server --webhook--> Rasa
  hq-server is the ONLY caller of Rasa. It forwards, in `metadata`:
    patient_token  — the same JWT the patient's own request was authenticated
                      with (see chatbotController.handleMessage)
    patient_id     — req.user._id
    patient_name   — req.user.fullName
    clinic_id      — hq-server's own resolvedClinicId (active queue/appt)
  action_session_start reads these into slots. Every action below calls
  hq-server back using patient_token, so hq-server sees these calls exactly
  as if the patient's own app made them (protect + patientOnly middleware
  all apply normally — no new auth mechanism needed).

Environment variables:
  HQ_SERVER_URL   — hq-server base URL, e.g. https://api.healthqueue.org/api
  HQ_API_TIMEOUT  — request timeout in seconds (default: 8)
"""

from typing import Any, Text, Dict, List, Optional
from rasa_sdk import Action, Tracker
from rasa_sdk.forms import FormValidationAction
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import SlotSet, SessionStarted, ActionExecuted, EventType
from datetime import datetime, timedelta
import requests
import os
import re
import logging

try:
    from pymongo import MongoClient
except ImportError:  # HTTP directory fallback remains available if dependency is missing.
    MongoClient = None

logger = logging.getLogger(__name__)

HQ_SERVER = os.getenv("HQ_SERVER_URL", "http://localhost:4000/api").rstrip("/")
TIMEOUT   = int(os.getenv("HQ_API_TIMEOUT", "8"))

URGENT_KEYWORDS = [
    "emergency", "urgent", "severe pain", "bleeding", "can't breathe",
    "chest pain", "allergic reaction", "sumasakit ng husto", "nahihirapan huminga",
    "hindi na kaya", "malubha", "grabe ang sakit",
]

# The available-slots endpoint (appointmentController.getAvailableSlots) only
# ever returns a subset of this fixed list — mirroring it here lets us show
# sensible options even before a date is picked.
DEFAULT_TIME_SLOTS = [
    "8:00 AM", "8:30 AM", "9:00 AM", "9:30 AM", "10:00 AM", "10:30 AM",
    "11:00 AM", "11:30 AM", "1:00 PM", "1:30 PM", "2:00 PM", "2:30 PM",
    "3:00 PM", "3:30 PM", "4:00 PM", "4:30 PM",
]

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


# ── HTTP helpers ────────────────────────────────────────────────────────────

def _get_metadata(tracker: Tracker) -> Dict:
    """Raw metadata sent with the CURRENT incoming message. hq-server
    attaches this on every single /chatbot/message call (not just the
    first one in a session) — see chatbotController.handleMessage."""
    return tracker.latest_message.get("metadata") or {}


def _patient_token(tracker: Tracker) -> Optional[str]:
    """Prefers the current message's own metadata over the slot.
    action_session_start only runs once, at the very start of a session —
    if it ever misses (a Rasa-version metadata-timing quirk, a session
    that got carried over from before this was wired up, anything),
    _is_authenticated()/_auth_headers() would silently and permanently
    treat every action for the rest of that session as logged-out, with
    no way to recover short of the session expiring. Reading straight
    from this message's own metadata first means every single turn gets
    its own chance to establish identity, not just the first one.
    """
    meta = _get_metadata(tracker)
    return meta.get("patient_token") or tracker.get_slot("patient_token")


def _patient_id(tracker: Tracker) -> Optional[str]:
    meta = _get_metadata(tracker)
    pid = meta.get("patient_id") or tracker.get_slot("patient_id")
    return str(pid) if pid else None


def _patient_name(tracker: Tracker) -> Optional[str]:
    meta = _get_metadata(tracker)
    return meta.get("patient_name") or tracker.get_slot("patient_name")


def _metadata_clinic_id(tracker: Tracker) -> Optional[str]:
    meta = _get_metadata(tracker)
    cid = meta.get("clinic_id") or tracker.get_slot("last_clinic_id")
    return str(cid) if cid else None


def _auth_headers(tracker: Tracker) -> Dict[str, str]:
    h = {"Content-Type": "application/json"}
    token = _patient_token(tracker)
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _get(path: str, tracker: Tracker, params: Optional[Dict] = None) -> Optional[Dict]:
    try:
        r = requests.get(f"{HQ_SERVER}/{path.lstrip('/')}", headers=_auth_headers(tracker),
                          params=params or {}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        logger.warning("[HQ-Rasa] GET %s -> %s", path, e.response.status_code)
        return None
    except Exception as e:
        logger.error("[HQ-Rasa] GET %s failed: %s", path, e)
        return None


def _post(path: str, data: Dict, tracker: Tracker) -> Optional[Dict]:
    try:
        r = requests.post(f"{HQ_SERVER}/{path.lstrip('/')}", headers=_auth_headers(tracker),
                           json=data, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        logger.warning("[HQ-Rasa] POST %s -> %s %s", path, e.response.status_code, e.response.text[:200])
        return None
    except Exception as e:
        logger.error("[HQ-Rasa] POST %s failed: %s", path, e)
        return None


def _put(path: str, data: Dict, tracker: Tracker) -> Optional[Dict]:
    try:
        r = requests.put(f"{HQ_SERVER}/{path.lstrip('/')}", headers=_auth_headers(tracker),
                          json=data, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        logger.warning("[HQ-Rasa] PUT %s -> %s %s", path, e.response.status_code, e.response.text[:200])
        return None
    except Exception as e:
        logger.error("[HQ-Rasa] PUT %s failed: %s", path, e)
        return None


def _is_authenticated(tracker: Tracker) -> bool:
    return bool(_patient_token(tracker))


def _need_login_message() -> str:
    return (
        "I need to confirm your account first — please make sure you're chatting "
        "from inside the HealthQueue+ app while logged in, then try again."
    )


def _fallback_message() -> str:
    return (
        "I'm having trouble reaching the clinic system right now. Please try again "
        "in a moment, or ask to speak with staff."
    )


# ── Clinic lookup (MongoDB first, HTTP directory fallback) ───────────────────

_MONGO_CLIENT = None
_MONGO_DB = None

# Words that add little or no identifying value when patients name a branch.
_CLINIC_NOISE_WORDS = {
    "hi", "precision", "diagnostics", "diagnostic", "branch", "clinic",
    "the", "at", "in", "of", "and", "health", "center", "centre",
}

def _normalize_clinic_text(value: Any) -> str:
    """Normalize a patient/DB clinic name for tolerant comparison."""
    if value is None:
        return ""
    text = str(value).lower().strip()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [
        token for token in text.split()
        if token and token not in _CLINIC_NOISE_WORDS
    ]
    return " ".join(tokens)


def _clinic_tokens(value: Any) -> set:
    return set(_normalize_clinic_text(value).split())


def _mongo_database():
    """Return the configured MongoDB database, or None if unavailable.

    The action server deliberately does not share the Node/Mongoose connection:
    pymongo owns its own connection pool. If the URI has a default database,
    get_default_database() uses it; otherwise we use the database component in
    the URI when present. Any connection/query failure is handled by the HTTP
    directory fallback in _load_clinics().
    """
    global _MONGO_CLIENT, _MONGO_DB

    if MongoClient is None:
        logger.warning("[HQ-Rasa] pymongo is not installed; using HTTP clinic fallback.")
        return None

    uri = os.getenv("MONGODB_URI")
    if not uri:
        logger.warning("[HQ-Rasa] MONGODB_URI is not configured; using HTTP clinic fallback.")
        return None

    try:
        if _MONGO_DB is not None:
            return _MONGO_DB

        _MONGO_CLIENT = MongoClient(
            uri,
            serverSelectionTimeoutMS=max(1000, min(TIMEOUT * 1000, 5000)),
            connectTimeoutMS=max(1000, min(TIMEOUT * 1000, 5000)),
        )
        # Force a cheap connectivity check once, rather than discovering a
        # dead Mongo connection only after several actions have timed out.
        _MONGO_CLIENT.admin.command("ping")

        try:
            _MONGO_DB = _MONGO_CLIENT.get_default_database()
        except Exception:
            _MONGO_DB = None

        if _MONGO_DB is None:
            # If the URI has no database component, use the conventional
            # HealthQueue+ database name rather than guessing from credentials.
            from urllib.parse import urlparse, unquote
            path = urlparse(uri).path.strip("/")
            db_name = unquote(path.split("/")[0]) if path else ""
            if not db_name:
                db_name = os.getenv("MONGODB_DB", "HQ_DB")
            _MONGO_DB = _MONGO_CLIENT[db_name]

        return _MONGO_DB
    except Exception as exc:
        logger.warning("[HQ-Rasa] MongoDB clinic lookup unavailable: %s", exc)
        _MONGO_CLIENT = None
        _MONGO_DB = None
        return None


def _load_clinics(tracker: Tracker) -> List[Dict]:
    """Load active clinics from MongoDB, then fall back to /clinics/directory."""
    db = _mongo_database()
    if db is not None:
        try:
            docs = list(db["clinics"].find({"isActive": {"$ne": False}}))
            if docs:
                return docs
            logger.warning("[HQ-Rasa] MongoDB returned no active clinics; trying HTTP directory.")
        except Exception as exc:
            logger.warning("[HQ-Rasa] MongoDB clinics query failed: %s; trying HTTP directory.", exc)

    data = _get("clinics/directory", tracker)
    if not data:
        return []
    clinics = data.get("data")
    if isinstance(clinics, list):
        return clinics
    clinics = data.get("clinics")
    return clinics if isinstance(clinics, list) else []


def _find_clinic(clinic_name: Optional[str], tracker: Tracker) -> Optional[Dict]:
    """Resolve colloquial branch names to a canonical clinic document.

    Matching is intentionally tolerant:
      1. normalize punctuation/case and remove generic clinic words;
      2. exact normalized match;
      3. normalized substring match;
      4. keyword-set intersection with a score favoring coverage and specificity.

    Examples:
      "Vertis" -> "Hi-Precision - Vertis"
      "hi precision vertis" -> "Hi-Precision - Vertis"
      "Vertis branch" -> "Hi-Precision - Vertis"
    """
    if not clinic_name or not str(clinic_name).strip():
        return None

    clinics = _load_clinics(tracker)
    if not clinics:
        logger.warning("[HQ-Rasa] _find_clinic(%r): no clinic records available.", clinic_name)
        return None

    query_norm = _normalize_clinic_text(clinic_name)
    query_tokens = _clinic_tokens(clinic_name)
    if not query_norm:
        return None

    scored = []
    for clinic in clinics:
        canonical = clinic.get("name") or clinic.get("clinicName") or ""
        candidate_norm = _normalize_clinic_text(canonical)
        candidate_tokens = _clinic_tokens(canonical)
        if not candidate_norm:
            continue

        score = 0.0

        if query_norm == candidate_norm:
            score += 1000

        if query_norm in candidate_norm:
            score += 500 + min(len(query_norm), 100)

        if candidate_norm in query_norm:
            score += 350 + min(len(candidate_norm), 100)

        overlap = query_tokens & candidate_tokens
        if overlap:
            # Reward shared identifying words, but penalize a match that only
            # shares a generic one-word location when a more specific query
            # exists.
            coverage = len(overlap) / max(len(query_tokens), 1)
            specificity = sum(len(token) for token in overlap)
            score += coverage * 200 + specificity

        if score > 0:
            scored.append((score, len(overlap), clinic))

    if not scored:
        logger.info("[HQ-Rasa] No clinic match for %r", clinic_name)
        return None

    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    clinic = scored[0][2]

    # Always return the canonical DB name and string ID. This is important:
    # downstream queue/appointment actions must use the actual Mongo _id, not
    # the patient's colloquial branch spelling.
    if clinic.get("_id") is not None:
        clinic["_id"] = clinic["_id"]

    logger.info(
        "[HQ-Rasa] _find_clinic(%r) -> %r (%s)",
        clinic_name,
        clinic.get("name"),
        str(clinic.get("_id", "")),
    )
    return clinic


def _format_wait(minutes: int) -> str:
    if not minutes or minutes <= 0:
        return "no wait"
    if minutes < 60:
        return f"~{minutes} minute{'s' if minutes != 1 else ''}"
    h, m = divmod(minutes, 60)
    return f"~{h}h {m}min" if m else f"~{h} hour{'s' if h != 1 else ''}"


# ── Date/time parsing for the appointment form ──────────────────────────────

def _parse_date_to_iso(text: str) -> Optional[str]:
    """Best-effort parse of common English/Taglish date phrasing to YYYY-MM-DD.
    Only handles the common cases (today/tomorrow/weekday/explicit date) — good
    enough for a chat prompt; the server still validates the final date."""
    if not text:
        return None
    t = text.strip().lower()
    today = datetime.now()

    if t in ("today", "ngayon", "ngayong araw"):
        return today.strftime("%Y-%m-%d")
    if t in ("tomorrow", "bukas"):
        return (today + timedelta(days=1)).strftime("%Y-%m-%d")

    for i, wd in enumerate(WEEKDAYS):
        if wd in t:
            days_ahead = (i - today.weekday()) % 7
            if days_ahead == 0 or "next" in t:
                days_ahead += 7
            return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    # Explicit formats: "2026-09-10", "09/10/2026", "Sept 10", "Sept 10 2026"
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d %Y", "%B %d, %Y", "%b %d %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # "Sept 10" / "September 10" without a year — assume this year, roll to
    # next year if that date already passed.
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})$", text.strip())
    if m:
        for fmt in ("%B %d", "%b %d"):
            try:
                parsed = datetime.strptime(m.group(0), fmt).replace(year=today.year)
                if parsed.date() < today.date():
                    parsed = parsed.replace(year=today.year + 1)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


def _match_time_slot(text: str, available: List[str]) -> Optional[str]:
    """Fuzzy-match free text like '10am' or '10:00' against the slot list."""
    norm = re.sub(r"[\s.]", "", text.strip().lower())
    for slot in available:
        if re.sub(r"[\s.]", "", slot.lower()) == norm:
            return slot
    digits = re.sub(r"[^0-9apm]", "", norm)
    for slot in available:
        if re.sub(r"[^0-9apm]", "", slot.lower()) == digits:
            return slot
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Session start — identify patient + personalize greeting
# ─────────────────────────────────────────────────────────────────────────────
class ActionSessionStart(Action):
    def name(self) -> Text:
        return "action_session_start"

    async def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]):
        events: List[EventType] = [SessionStarted()]

        # Still set from metadata into slots here (for carry-over across
        # this session, and so slots like patient_name are available to
        # any code that only reads slots) — but this is no longer the
        # ONLY place identity is established. _patient_token/_patient_id/
        # _patient_name (used by every action below) read the CURRENT
        # message's metadata first and only fall back to these slots, so
        # a miss here isn't fatal for the rest of the session anymore.
        metadata = tracker.latest_message.get("metadata") or {}
        token        = metadata.get("patient_token")
        patient_id   = metadata.get("patient_id")
        patient_name = metadata.get("patient_name")
        clinic_id    = metadata.get("clinic_id")

        if token:
            events.append(SlotSet("patient_token", token))
        if patient_id:
            events.append(SlotSet("patient_id", str(patient_id)))
        if patient_name:
            events.append(SlotSet("patient_name", patient_name))
        if clinic_id:
            events.append(SlotSet("last_clinic_id", str(clinic_id)))
        events.append(SlotSet("escalated", False))

        first_name = (patient_name or "").split(" ")[0] if patient_name else ""
        greeting = (
            f"Hi{', ' + first_name if first_name else ''}! 👋 I'm the HealthQueue+ Assistant. "
            "I can check wait times, get you a queue number, book or manage appointments, "
            "and answer clinic questions."
        )

        if token:
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            try:
                r = requests.get(f"{HQ_SERVER}/queues/my-status", headers=headers, timeout=TIMEOUT)
                q = r.json() if r.ok else None
            except Exception:
                q = None
            try:
                r = requests.get(f"{HQ_SERVER}/appointments/my", headers=headers, timeout=TIMEOUT)
                a = r.json() if r.ok else None
            except Exception:
                a = None

            if q and q.get("activeQueue") and q.get("entry"):
                entry = q["entry"]
                cname = (entry.get("clinic") or {}).get("name", "your clinic")
                greeting += (
                    f"\n\nYou're currently **#{entry.get('queueNumber', '?')}** in the queue "
                    f"at {cname} — want a status update?"
                )
            elif a and a.get("appointments"):
                upcoming = [x for x in a["appointments"] if x.get("status") in ("pending", "confirmed")]
                if upcoming:
                    nxt = upcoming[0]
                    clinic_name = (nxt.get("clinic") or {}).get("name", "the clinic")
                    date_str = (nxt.get("appointmentDate") or "")[:10]
                    greeting += (
                        f"\n\nQuick reminder: you have **{nxt.get('serviceName', 'an appointment')}** "
                        f"on **{date_str} {nxt.get('timeSlot', '')}** at {clinic_name}."
                    )

        dispatcher.utter_message(text=greeting)
        events.append(ActionExecuted("action_listen"))
        return events


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Estimated wait time / clinic recommendation
# GET /clinics/recommend (public) -> {recommendation, nearestClinic, fastestClinic, clinics:[...]}
# each clinic item: name, city, status, queueLength, currentWaitingTime, distanceKm, avgWaitMinutes
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetEstimatedWaitTime(Action):
    def name(self) -> Text:
        return "action_get_estimated_wait_time"

    def run(self, dispatcher, tracker, domain):
        clinic_name = tracker.get_slot("clinic_name")
        service     = tracker.get_slot("service_name")

        if clinic_name:
            clinic = _find_clinic(clinic_name, tracker)
            if clinic:
                wait = _format_wait(clinic.get("currentWaitingTime", 0))
                dispatcher.utter_message(
                    text=(
                        f"Current wait at **{clinic['name']}**: **{wait}** "
                        f"with {clinic.get('queueLength', 0)} patient(s) in queue."
                    )
                )
                return [SlotSet("last_clinic_id", str(clinic.get("_id", "")))]

        params = {"service": service} if service else {}
        data = _get("clinics/recommend", tracker, params)
        clinics = (data or {}).get("clinics") or []
        if not clinics:
            dispatcher.utter_message(text=_fallback_message())
            return []

        top = clinics[0]
        wait = _format_wait(top.get("avgWaitMinutes", top.get("currentWaitingTime", 0)))
        dispatcher.utter_message(
            text=(
                f"The shortest wait right now is at **{top.get('name')}** ({top.get('city', '')}) "
                f"— {wait}. {(data.get('recommendation') or '').strip()} Want to join their queue?"
            )
        )
        return [SlotSet("last_clinic_id", str(top.get("_id", "")))]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Clinic recommendations (nearest_clinic / recommendation_request)
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetClinicRecommendations(Action):
    def name(self) -> Text:
        return "action_get_clinic_recommendations"

    def run(self, dispatcher, tracker, domain):
        service = tracker.get_slot("service_name")
        params = {"service": service} if service else {}
        data = _get("clinics/recommend", tracker, params)
        clinics = (data or {}).get("clinics") or []

        if not clinics:
            dispatcher.utter_message(text="I couldn't fetch clinic recommendations right now — please try the app's 'AI Suggest' option.")
            return []

        svc_part = f" for **{service}**" if service else ""
        lines = [f"Here are the top recommended clinics{svc_part} right now:\n"]
        for i, c in enumerate(clinics[:3], 1):
            wait = _format_wait(c.get("avgWaitMinutes", c.get("currentWaitingTime", 0)))
            status = "🟢 Open" if c.get("status") in ("open", "active") else f"🔴 {c.get('status', 'closed').title()}"
            dist = f" · {c['distanceKm']:.1f} km away" if c.get("distanceKm") is not None else ""
            lines.append(f"**{i}. {c.get('name')}** ({c.get('city', '')}){dist}\n   {status} · Wait: {wait}\n")
        lines.append("\nWant to join the queue or book an appointment at one of these? Just say the clinic name.")
        dispatcher.utter_message(text="\n".join(lines))
        return [SlotSet("last_clinic_id", str(clinics[0].get("_id", "")))]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Clinic hours / services — GET /clinics/directory (public) or /clinics/:id
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetClinicHours(Action):
    def name(self) -> Text:
        return "action_get_clinic_hours"

    def run(self, dispatcher, tracker, domain):
        clinic_name = tracker.get_slot("clinic_name")
        clinic = _find_clinic(clinic_name, tracker) if clinic_name else None

        if clinic:
            status_str = "🟢 Currently open" if clinic.get("status") in ("open", "active") else f"🔴 Currently {clinic.get('status')}"
            dispatcher.utter_message(
                text=f"**{clinic['name']}** operating hours: **{clinic.get('operatingHours', '8:00 AM – 5:00 PM')}**. {status_str}."
            )
            return [SlotSet("last_clinic_id", str(clinic.get("_id", "")))]

        data = _get("clinics/directory", tracker)
        clinics = (data or {}).get("data", [])
        if clinics:
            lines = ["Here are the operating hours for our branches:\n"]
            for c in clinics[:5]:
                st = "🟢" if c.get("status") in ("open", "active") else "🔴"
                lines.append(f"{st} **{c['name']}**: {c.get('operatingHours', '8:00 AM – 5:00 PM')}")
            dispatcher.utter_message(text="\n".join(lines))
        else:
            dispatcher.utter_message(text="Branches are generally open **Monday–Saturday, 8:00 AM – 5:00 PM**.")
        return []


class ActionGetClinicServices(Action):
    def name(self) -> Text:
        return "action_get_clinic_services"

    def run(self, dispatcher, tracker, domain):
        clinic_name = tracker.get_slot("clinic_name")
        clinic_id = _metadata_clinic_id(tracker) or tracker.get_slot("last_clinic_id")

        clinic = _find_clinic(clinic_name, tracker) if clinic_name else None

        # If a clinic ID was supplied by the authenticated patient context but
        # no clinic name is set, resolve the exact record directly.
        if clinic is None and clinic_id:
            clinics = _load_clinics(tracker)
            clinic = next(
                (c for c in clinics if str(c.get("_id", "")) == str(clinic_id)),
                None,
            )

        if clinic:
            services = [
                s for s in (clinic.get("services") or [])
                if isinstance(s, dict) and s.get("isAvailable") is True
            ]
            service_names = [
                str(s.get("name", "")).strip()
                for s in services
                if str(s.get("name", "")).strip()
            ]
            if service_names:
                dispatcher.utter_message(
                    text=f"**{clinic.get('name', 'This branch')}** currently offers: "
                         f"{', '.join(service_names)}. Which one would you like?"
                )
            else:
                dispatcher.utter_message(
                    text=f"There are no currently available services listed for **{clinic.get('name', 'this branch')}**."
                )
            return [SlotSet("clinic_name", clinic.get("name")),
                    SlotSet("last_clinic_id", str(clinic.get("_id", "")))]

        dispatcher.utter_message(
            text="Please tell me the branch name first so I can show its current available services."
        )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Join / check / cancel queue — patient-authenticated
# ─────────────────────────────────────────────────────────────────────────────
class ValidateJoinQueueForm(FormValidationAction):
    """Validate the selected branch against the live clinic directory/MongoDB."""
    def name(self) -> Text:
        return "validate_join_queue_form"

    def validate_clinic_name(self, slot_value, dispatcher, tracker, domain):
        clinic = _find_clinic(slot_value, tracker)
        if not clinic:
            dispatcher.utter_message(
                text=f'I couldn\'t find a branch matching "{slot_value}". '
                     "Please give the branch name, or ask me to recommend one."
            )
            return {"clinic_name": None, "last_clinic_id": None}

        return {
            "clinic_name": clinic.get("name"),
            "last_clinic_id": str(clinic.get("_id", "")),
        }

    def validate_service_name(self, slot_value, dispatcher, tracker, domain):
        clinic = _find_clinic(tracker.get_slot("clinic_name"), tracker)
        service = _find_service_for_clinic(clinic, slot_value)
        if not service:
            dispatcher.utter_message(
                text=f'"{slot_value}" is not an available service at this branch.'
            )
            return {"service_name": None}
        return {"service_name": service.get("name")}


class ActionJoinQueue(Action):
    def name(self) -> Text:
        return "action_join_queue"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        # By the time this runs, join_queue_form (see rules.yml) has
        # already resolved clinic_name -> last_clinic_id via
        # ValidateJoinQueueForm — or the form skipped itself entirely
        # because last_clinic_id was already known (e.g. from a prior
        # recommendation). Either way, this no longer needs to ask "which
        # branch" itself — that was the actual bug: a bare clinic-name
        # reply to that question had nothing trained to capture it before
        # the form's from_text mapping existed.
        service   = tracker.get_slot("service_name")
        clinic_id = tracker.get_slot("last_clinic_id")

        if not clinic_id:
            dispatcher.utter_message(text="I still need a branch to queue you at — which one, or should I recommend one?")
            return []

        payload = {"clinicId": clinic_id}
        if service:
            payload["serviceName"] = service

        result = _post("queues/join", payload, tracker)

        if result and result.get("success") and result.get("entry"):
            entry = result["entry"]
            wait  = _format_wait(result.get("estimatedWaitTime", 0))
            dispatcher.utter_message(
                text=(
                    f"You've joined the queue! 🎉 Queue **#{entry['queueNumber']}** at "
                    f"{entry.get('clinicName', '')} for {entry.get('serviceName', service or 'your visit')}. "
                    f"Position: {result.get('position', '?')}. Estimated wait: **{wait}**."
                )
            )
            return [SlotSet("last_queue_number", entry["queueNumber"]), SlotSet("last_clinic_id", clinic_id)]

        msg = (result or {}).get("message", "")
        if "already have an active" in msg.lower():
            dispatcher.utter_message(text="You're already in the queue at this clinic today — say 'check my queue' for your status.")
        elif "closed" in msg.lower():
            dispatcher.utter_message(text=f"{msg} Want a recommendation for another branch?")
        else:
            dispatcher.utter_message(text=_fallback_message())
        return []


class ActionCheckQueueStatus(Action):
    def name(self) -> Text:
        return "action_check_queue_status"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        data = _get("queues/my-status", tracker)
        if data is None:
            dispatcher.utter_message(text=_fallback_message())
            return []

        if not data.get("activeQueue"):
            dispatcher.utter_message(text="You're not currently in any queue today. Want to join one or book an appointment?")
            return []

        entry  = data.get("entry", {})
        clinic = entry.get("clinic") or {}
        cname  = clinic.get("name", "your clinic")
        qnum   = entry.get("queueNumber", "?")
        status = entry.get("status", "waiting")
        wait   = _format_wait(data.get("estimatedWaitTime", 0))

        if status == "serving":
            dispatcher.utter_message(text=f"You are currently being served (Queue **#{qnum}**) at {cname}. Please proceed to the counter.")
        elif status == "called":
            dispatcher.utter_message(text=f"🔔 You're being called! Queue **#{qnum}** at {cname} — please head to the counter now.")
        else:
            dispatcher.utter_message(
                text=(
                    f"You are **#{qnum}** in the queue at {cname}. "
                    f"Position: {data.get('position', '?')} ({data.get('peopleAhead', 0)} ahead). "
                    f"Estimated wait: **{wait}**."
                )
            )
        return [
            SlotSet("last_queue_number", qnum),
            SlotSet("pending_queue_entry_id", entry.get("_id")),
            SlotSet("last_clinic_id", str(clinic.get("_id", ""))),
        ]


class ActionCancelQueue(Action):
    def name(self) -> Text:
        return "action_cancel_queue"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        data = _get("queues/my-status", tracker)
        if data and data.get("activeQueue"):
            entry = data.get("entry", {})
            cname = (entry.get("clinic") or {}).get("name", "your clinic")
            dispatcher.utter_message(
                text=f"You are currently **#{entry.get('queueNumber', '?')}** at {cname}. Cancel and lose your spot? (yes/no)"
            )
            # pending_confirmation disambiguates a bare "yes"/"no" between
            # this and ActionCancelAppointment below — both are triggered
            # by the same affirm/deny intent, so Rasa's RulePolicy needs a
            # single slot with distinct VALUES ('cancel_queue' vs
            # 'cancel_appointment') to prove the two rules can't both match
            # at once. Just checking "was pending_queue_entry_id set" isn't
            # enough — Rasa can't verify that's mutually exclusive with
            # "was pending_appointment_id set" from the rule alone.
            return [
                SlotSet("pending_queue_entry_id", entry.get("_id")),
                SlotSet("pending_confirmation", "cancel_queue"),
            ]

        dispatcher.utter_message(text="You don't have an active queue entry to cancel today.")
        return []


class ActionConfirmCancelQueue(Action):
    def name(self) -> Text:
        return "action_confirm_cancel_queue"

    def run(self, dispatcher, tracker, domain):
        entry_id = tracker.get_slot("pending_queue_entry_id")
        if not entry_id:
            dispatcher.utter_message(text="There's nothing pending to cancel.")
            return []
        result = _put(f"queues/{entry_id}/cancel", {}, tracker)
        if result and result.get("success"):
            dispatcher.utter_message(text="Done — you've been removed from the queue. You can rejoin anytime.")
        else:
            dispatcher.utter_message(text=_fallback_message())
        return [SlotSet("pending_queue_entry_id", None), SlotSet("pending_confirmation", None)]


# ─────────────────────────────────────────────────────────────────────────────
# APPOINTMENT BOOKING FORM
# clinic_name -> service_name -> date (free text -> parsed ISO)
# -> time (matched against real available-slots for that date)
# -> POST /appointments {clinicId, serviceName, appointmentDate, timeSlot}
#
# Shared by both appointment_form (clinic_name, service_name, date, time)
# and reschedule_form (date, time only) — factored into a helper so both
# FormValidationAction classes below (Rasa requires one named
# validate_<form_name> per form) share identical date-parsing and
# availability-checking behavior instead of duplicating it.
# ─────────────────────────────────────────────────────────────────────────────

def _validate_date_slot(slot_value, dispatcher, tracker):
    """Parse an ISO-compatible date and verify real slots through hq-server.

    We do NOT silently fall back to DEFAULT_TIME_SLOTS when the API is down:
    accepting a time based on stale/static data would make the form claim a
    slot is bookable when the backend cannot confirm it.
    """
    iso = _parse_date_to_iso(str(slot_value or ""))
    if not iso:
        dispatcher.utter_message(
            text='I couldn\'t understand that date. Please use a date like '
                 '"tomorrow", "next Monday", or "September 10".'
        )
        return {"date": None, "available_slots": None}

    clinic_id = tracker.get_slot("last_clinic_id") or _metadata_clinic_id(tracker)
    if not clinic_id:
        dispatcher.utter_message(
            text="I need the clinic first so I can check its available appointment times."
        )
        return {"date": None, "available_slots": None}

    data = _get(
        "appointments/available-slots",
        tracker,
        {"clinicId": str(clinic_id), "date": iso},
    )

    if not data or data.get("success") is False:
        dispatcher.utter_message(
            text="I couldn't check the live appointment slots right now. Please try the date again in a moment."
        )
        return {"date": None, "available_slots": None}

    available = data.get("data")
    if not isinstance(available, list):
        available = data.get("slots")

    if not isinstance(available, list):
        dispatcher.utter_message(
            text="I couldn't get the clinic's available times right now. Please try again."
        )
        return {"date": None, "available_slots": None}

    available = [str(slot).strip() for slot in available if str(slot).strip()]
    if not available:
        dispatcher.utter_message(
            text=f"No appointment slots are available on **{iso}**. Would you like to try another date?"
        )
        return {"date": None, "available_slots": None}

    dispatcher.utter_message(
        text=f"Available times on **{iso}**: {', '.join(available[:16])}"
    )
    return {"date": iso, "available_slots": available}


def _validate_time_slot(slot_value, dispatcher, tracker):
    """Accept only a time returned by the live availability endpoint."""
    available = tracker.get_slot("available_slots") or []
    if not available:
        dispatcher.utter_message(
            text="I don't have a confirmed list of available times yet. Please provide the date again."
        )
        return {"time": None}

    matched = _match_time_slot(str(slot_value or ""), available)
    if not matched:
        dispatcher.utter_message(
            text=f"That time is not available. Please choose one of: {', '.join(available[:16])}"
        )
        return {"time": None}

    return {"time": matched}


class ActionAskTime(Action):
    """Show the actual available times returned for the selected date."""
    def name(self) -> Text:
        return "action_ask_time"

    def run(self, dispatcher, tracker, domain):
        available = tracker.get_slot("available_slots") or []
        if available:
            dispatcher.utter_message(
                text=f"Which time works for you? ({', '.join(available[:16])})"
            )
        else:
            dispatcher.utter_message(text="What time would you like?")
        return []


def _find_service_for_clinic(clinic: Optional[Dict], service_name: Any) -> Optional[Dict]:
    """Resolve a patient service/test name against the selected clinic's live services."""
    if not clinic or not service_name:
        return None

    query = str(service_name).strip()
    qnorm = re.sub(r"[^a-z0-9]+", " ", query.lower()).strip()
    qtokens = set(qnorm.split())
    if not qnorm:
        return None

    best = None
    best_score = 0.0

    for service in clinic.get("services") or []:
        if not isinstance(service, dict) or service.get("isAvailable") is not True:
            continue
        name = str(service.get("name", "")).strip()
        if not name:
            continue

        cnorm = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
        ctokens = set(cnorm.split())
        score = 0.0

        if qnorm == cnorm:
            score += 1000
        if qnorm in cnorm:
            score += 500
        if cnorm in qnorm:
            score += 350

        overlap = qtokens & ctokens
        if overlap:
            score += (len(overlap) / max(len(qtokens), 1)) * 200
            score += sum(len(x) for x in overlap)

        if score > best_score:
            best_score = score
            best = service

    return best


class ValidateAppointmentForm(FormValidationAction):
    def name(self) -> Text:
        return "validate_appointment_form"

    def validate_clinic_name(self, slot_value, dispatcher, tracker, domain):
        clinic = _find_clinic(slot_value, tracker)
        if not clinic:
            dispatcher.utter_message(
                text=f'I couldn\'t find a branch matching "{slot_value}". '
                     "Please check the branch name or ask me to recommend one."
            )
            return {"clinic_name": None, "last_clinic_id": None}

        return {
            "clinic_name": clinic.get("name"),
            "last_clinic_id": str(clinic.get("_id", "")),
        }

    def validate_service_name(self, slot_value, dispatcher, tracker, domain):
        clinic = _find_clinic(
            tracker.get_slot("clinic_name"),
            tracker,
        )
        if clinic is None:
            clinic_id = tracker.get_slot("last_clinic_id") or _metadata_clinic_id(tracker)
            if clinic_id:
                clinic = next(
                    (
                        c for c in _load_clinics(tracker)
                        if str(c.get("_id", "")) == str(clinic_id)
                    ),
                    None,
                )

        service = _find_service_for_clinic(clinic, slot_value)
        if not service:
            available = [
                str(s.get("name", "")).strip()
                for s in (clinic or {}).get("services", [])
                if isinstance(s, dict)
                and s.get("isAvailable") is True
                and str(s.get("name", "")).strip()
            ]
            suffix = f" Available services: {', '.join(available[:12])}." if available else ""
            dispatcher.utter_message(
                text=f'"{slot_value}" is not an available service at this branch.{suffix}'
            )
            return {"service_name": None}

        return {"service_name": service.get("name")}

    def validate_date(self, slot_value, dispatcher, tracker, domain):
        return _validate_date_slot(slot_value, dispatcher, tracker)

    def validate_time(self, slot_value, dispatcher, tracker, domain):
        return _validate_time_slot(slot_value, dispatcher, tracker)


class ValidateRescheduleForm(FormValidationAction):
    def name(self) -> Text:
        return "validate_reschedule_form"

    def validate_date(self, slot_value, dispatcher, tracker, domain):
        return _validate_date_slot(slot_value, dispatcher, tracker)

    def validate_time(self, slot_value, dispatcher, tracker, domain):
        return _validate_time_slot(slot_value, dispatcher, tracker)


class ActionSubmitAppointmentForm(Action):
    def name(self) -> Text:
        return "action_submit_appointment_form"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        clinic_id = tracker.get_slot("last_clinic_id")
        service   = tracker.get_slot("service_name")
        date_iso  = tracker.get_slot("date")
        time_slot = tracker.get_slot("time")

        result = _post("appointments", {
            "clinicId": clinic_id,
            "serviceName": service,
            "appointmentDate": date_iso,
            "timeSlot": time_slot,
        }, tracker)

        if result and result.get("success") and result.get("appointment"):
            a = result["appointment"]
            dispatcher.utter_message(
                text=(
                    f"You're booked! ✅ **{a.get('serviceName', service)}** at **{a.get('clinicName', '')}** "
                    f"on **{str(a.get('appointmentDate', date_iso))[:10]} {a.get('timeSlot', time_slot)}**."
                )

            )
        else:
            msg = (result or {}).get("message", "")
            if "already have an appointment" in msg.lower():
                dispatcher.utter_message(text="You already have an appointment at that exact time — want a different slot?")
            else:
                dispatcher.utter_message(text=_fallback_message())

        return [SlotSet("available_slots", None)]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: My appointments / cancel / reschedule
# GET /appointments/my, PUT /appointments/:id/cancel, PUT /appointments/:id
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetMyAppointments(Action):
    def name(self) -> Text:
        return "action_get_my_appointments"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        data = _get("appointments/my", tracker)
        appts = [a for a in (data or {}).get("appointments", []) if a.get("status") in ("pending", "confirmed")]

        if not appts:
            dispatcher.utter_message(text="You have no upcoming appointments. Want to book one?")
            return []

        lines = ["Your upcoming appointments:\n"]
        for a in appts[:5]:
            cname = (a.get("clinic") or {}).get("name", "")
            lines.append(f"• {a.get('serviceName')} — {cname} on {str(a.get('appointmentDate', ''))[:10]} {a.get('timeSlot', '')}")
        dispatcher.utter_message(text="\n".join(lines))
        return [SlotSet("pending_appointment_id", appts[0].get("_id"))]


class ActionCancelAppointment(Action):
    def name(self) -> Text:
        return "action_cancel_appointment"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        data = _get("appointments/my", tracker)
        appts = [a for a in (data or {}).get("appointments", []) if a.get("status") in ("pending", "confirmed")]

        if not appts:
            dispatcher.utter_message(text="You don't have any upcoming appointments to cancel.")
            return []

        if len(appts) == 1:
            a = appts[0]
            cname = (a.get("clinic") or {}).get("name", "")
            dispatcher.utter_message(
                text=f"You have **{a.get('serviceName')}** at **{cname}** on "
                     f"**{str(a.get('appointmentDate',''))[:10]} {a.get('timeSlot','')}**. Cancel it? (yes/no)"
            )
            # See the matching comment in ActionCancelQueue — pending_confirmation
            # is what lets Rasa tell this affirm/deny apart from a queue
            # cancellation confirmation without a rule contradiction.
            return [
                SlotSet("pending_appointment_id", a.get("_id")),
                SlotSet("pending_confirmation", "cancel_appointment"),
            ]

        lines = ["Which appointment would you like to cancel?\n"]
        for i, a in enumerate(appts[:5], 1):
            cname = (a.get("clinic") or {}).get("name", "")
            lines.append(f"{i}. {a.get('serviceName')} — {cname} on {str(a.get('appointmentDate',''))[:10]} {a.get('timeSlot','')}")
        dispatcher.utter_message(text="\n".join(lines))
        return []


class ActionConfirmCancelAppointment(Action):
    def name(self) -> Text:
        return "action_confirm_cancel_appointment"

    def run(self, dispatcher, tracker, domain):
        appt_id = tracker.get_slot("pending_appointment_id")
        if not appt_id:
            dispatcher.utter_message(text="There's nothing pending to cancel.")
            return []
        result = _put(f"appointments/{appt_id}/cancel", {}, tracker)
        if result and result.get("success"):
            dispatcher.utter_message(text="Your appointment has been cancelled. Want to book a new one?")
        else:
            dispatcher.utter_message(text=_fallback_message())
        return [SlotSet("pending_appointment_id", None), SlotSet("pending_confirmation", None)]


class ActionClearPendingConfirmation(Action):
    """Handles a 'no' to either pending cancel confirmation. One shared
    action (rather than two separate deny actions) because the meaningful
    difference is just which message to show — the cleanup is identical."""

    def name(self) -> Text:
        return "action_clear_pending_confirmation"

    def run(self, dispatcher, tracker, domain):
        kind = tracker.get_slot("pending_confirmation")
        if kind == "cancel_queue":
            dispatcher.utter_message(text="No problem — your queue spot is still active.")
        elif kind == "cancel_appointment":
            dispatcher.utter_message(text="No problem — your appointment is still booked.")
        return [
            SlotSet("pending_confirmation", None),
            SlotSet("pending_queue_entry_id", None),
            SlotSet("pending_appointment_id", None),
        ]


class ActionRescheduleAppointment(Action):
    """Step 1: pick the appointment, then the reschedule_form asks for a new date+time."""

    def name(self) -> Text:
        return "action_reschedule_appointment"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        data = _get("appointments/my", tracker)
        appts = [a for a in (data or {}).get("appointments", []) if a.get("status") in ("pending", "confirmed")]
        if not appts:
            dispatcher.utter_message(text="You don't have any upcoming appointments to reschedule.")
            return []

        a = appts[0]
        cname = (a.get("clinic") or {}).get("name", "")
        dispatcher.utter_message(
            text=f"You have **{a.get('serviceName')}** at **{cname}** on "
                 f"**{str(a.get('appointmentDate',''))[:10]} {a.get('timeSlot','')}**."
        )
        return [
            SlotSet("pending_appointment_id", a.get("_id")),
            SlotSet("last_clinic_id", str((a.get("clinic") or {}).get("_id", ""))),
            SlotSet("date", None),
            SlotSet("time", None),
        ]


class ActionConfirmReschedule(Action):
    def name(self) -> Text:
        return "action_confirm_reschedule"

    def run(self, dispatcher, tracker, domain):
        appt_id  = tracker.get_slot("pending_appointment_id")
        new_date = tracker.get_slot("date")
        new_time = tracker.get_slot("time")

        if not appt_id or not new_date or not new_time:
            dispatcher.utter_message(text=_fallback_message())
            return []

        result = _put(f"appointments/{appt_id}", {"appointmentDate": new_date, "timeSlot": new_time}, tracker)
        if result and result.get("success"):
            dispatcher.utter_message(text=f"Rescheduled! ✅ New date: **{new_date} {new_time}**.")
        else:
            dispatcher.utter_message(text=_fallback_message())
        return [SlotSet("pending_appointment_id", None), SlotSet("available_slots", None)]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Escalate to staff — POST /chatbot/escalate (patient-authenticated)
# ─────────────────────────────────────────────────────────────────────────────
class ActionEscalateToStaff(Action):
    def name(self) -> Text:
        return "action_escalate_to_staff"

    def run(self, dispatcher, tracker, domain):
        if not _is_authenticated(tracker):
            dispatcher.utter_message(text=_need_login_message())
            return []

        if tracker.get_slot("escalated"):
            dispatcher.utter_message(text="You're already connected to our staff team on this — they'll follow up shortly. Anything else to add?")
            return []

        events    = tracker.events
        last_msgs = [e.get("text", "") for e in events[-10:] if e.get("event") == "user" and e.get("text")]
        note = " | ".join(last_msgs[-3:]) if last_msgs else "Patient requested staff assistance"
        urgent = any(kw in note.lower() for kw in URGENT_KEYWORDS)

        payload = {"note": note}
        clinic_id = tracker.get_slot("last_clinic_id")
        if clinic_id:
            payload["clinicId"] = clinic_id

        result = _post("chatbot/escalate", payload, tracker)

        if result and result.get("success"):
            if urgent:
                dispatcher.utter_message(
                    text=(
                        "I've flagged this to our staff as urgent — someone will reach out right away. "
                        "**If this is a medical emergency, please call your branch or go to the nearest ER now.**"
                    )
                )
            else:
                dispatcher.utter_message(text="I've flagged your concern to our staff — they'll follow up shortly. Anything else to add in the meantime?")
            return [SlotSet("escalated", True)]

        if result and result.get("requiresClinic"):
            dispatcher.utter_message(
                text="To connect you with staff, please join a queue or book an appointment first so I know which clinic to alert."
            )
            return []

        dispatcher.utter_message(text="Please contact your clinic branch directly, or use the **Contact** section in the app.")
        return []
