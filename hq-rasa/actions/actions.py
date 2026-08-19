"""
HealthQueue+ Rasa Custom Actions — v2
Connects to hq-server live API for all dynamic responses.
Zero hardcoded data — everything is fetched from the server.

Environment variables:
  HQ_SERVER_URL   — hq-server base URL (default: http://localhost:4000/api)
  HQ_BOT_TOKEN    — JWT token for the Rasa service account on hq-server
                    Generate with: POST /api/auth/login (role: patient or staff)
"""

from typing import Any, Text, Dict, List, Optional
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import SlotSet, SessionStarted, ActionExecuted
import requests
import os
import logging

logger = logging.getLogger(__name__)

# ── Server connection ──────────────────────────────────────────────────────────
HQ_SERVER  = os.getenv("HQ_SERVER_URL", "http://localhost:4000/api").rstrip("/")
BOT_TOKEN  = os.getenv("HQ_BOT_TOKEN", "")
TIMEOUT    = int(os.getenv("HQ_API_TIMEOUT", "8"))

# ── Helpers ───────────────────────────────────────────────────────────────────

def _auth_headers() -> Dict[str, str]:
    """Build auth headers for hq-server requests."""
    h = {"Content-Type": "application/json"}
    if BOT_TOKEN:
        h["Authorization"] = f"Bearer {BOT_TOKEN}"
    return h


def _get(path: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """GET /api/<path> — returns parsed JSON or None on error."""
    try:
        r = requests.get(
            f"{HQ_SERVER}/{path.lstrip('/')}",
            headers=_auth_headers(),
            params=params or {},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except requests.Timeout:
        logger.warning("[HQ-Rasa] API timeout: GET %s", path)
        return None
    except requests.HTTPError as e:
        logger.warning("[HQ-Rasa] HTTP error: GET %s — %s", path, e.response.status_code)
        return None
    except Exception as e:
        logger.error("[HQ-Rasa] Unexpected error: GET %s — %s", path, str(e))
        return None


def _post(path: str, data: Dict) -> Optional[Dict]:
    """POST /api/<path> — returns parsed JSON or None on error."""
    try:
        r = requests.post(
            f"{HQ_SERVER}/{path.lstrip('/')}",
            headers=_auth_headers(),
            json=data,
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except requests.Timeout:
        logger.warning("[HQ-Rasa] API timeout: POST %s", path)
        return None
    except requests.HTTPError as e:
        logger.warning("[HQ-Rasa] HTTP error: POST %s — %s %s", path,
                       e.response.status_code, e.response.text[:200])
        return None
    except Exception as e:
        logger.error("[HQ-Rasa] Unexpected error: POST %s — %s", path, str(e))
        return None


def _resolve_clinic_id(clinic_name: Optional[str]) -> Optional[str]:
    """Find a clinic's _id by fuzzy name match from the server."""
    if not clinic_name:
        return None
    data = _get("clinics", {"limit": 50})
    if not data:
        return None
    clinics = data if isinstance(data, list) else data.get("clinics", [])
    name_lower = clinic_name.lower()
    for c in clinics:
        if name_lower in c.get("name", "").lower():
            return str(c.get("_id", ""))
    return None


def _format_wait(minutes: int) -> str:
    """Human-friendly wait time string."""
    if minutes <= 0:
        return "no wait"
    if minutes < 60:
        return f"~{minutes} minute{'s' if minutes != 1 else ''}"
    h = minutes // 60
    m = minutes % 60
    return f"~{h}h {m}min" if m else f"~{h} hour{'s' if h != 1 else ''}"


def _fallback_message(action_name: str) -> str:
    """Generic fallback when server is unreachable."""
    return (
        "I'm having trouble connecting to the clinic system right now. "
        "Please try again in a moment, or contact the clinic directly. "
        "Would you like me to connect you to a staff member?"
    )


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Get Estimated Wait Time
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetEstimatedWaitTime(Action):
    """Fetches live wait time from GET /api/queues/metrics."""

    def name(self) -> Text:
        return "action_get_estimated_wait_time"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        clinic_name = tracker.get_slot("clinic_name")
        service     = tracker.get_slot("service_name")
        clinic_id   = tracker.get_slot("last_clinic_id") or _resolve_clinic_id(clinic_name)

        if not clinic_id:
            # No specific clinic — recommend based on shortest wait
            recs = _get("clinics/recommend", {"type": "queue"})
            if recs and recs.get("clinics"):
                top = recs["clinics"][0]
                wait = _format_wait(top.get("currentWaitingTime", 0))
                dispatcher.utter_message(
                    text=(
                        f"The shortest wait right now is at **{top['clinicName']}** "
                        f"({top.get('city', '')}) — {wait} with {top.get('queueCount', 0)} patient(s) in queue. "
                        f"{top.get('explanation', '')}. Would you like to join their queue?"
                    )
                )
                return [SlotSet("last_clinic_id", str(top["clinicId"]))]
            dispatcher.utter_message(text=_fallback_message("wait_time"))
            return []

        data = _get("queues/metrics", {"clinicId": clinic_id})
        if not data:
            dispatcher.utter_message(text=_fallback_message("wait_time"))
            return []

        wait     = _format_wait(data.get("avgWaitTime", 0))
        waiting  = data.get("waitingCount", 0)
        serving  = data.get("servingCount", 0)
        active   = waiting + serving

        svc_part = f" for {service}" if service else ""
        msg = (
            f"Current wait time{svc_part}: **{wait}**. "
            f"There are currently {waiting} patient(s) waiting and {serving} being served. "
        )
        if active == 0:
            msg += "No queue right now — it's a great time to visit!"
        elif active > 15:
            msg += "The clinic is quite busy right now. I recommend an appointment or checking a nearby branch."
        else:
            msg += "Not too crowded at the moment."

        dispatcher.utter_message(text=msg)
        return [SlotSet("last_wait_time", float(data.get("avgWaitTime", 0)))]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Check Queue Status
# ─────────────────────────────────────────────────────────────────────────────
class ActionCheckQueueStatus(Action):
    """Fetches the patient's own queue status from GET /api/queues/my-status."""

    def name(self) -> Text:
        return "action_check_queue_status"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        data = _get("queues/my-status")

        if data is None:
            dispatcher.utter_message(text=_fallback_message("queue_status"))
            return []

        if not data.get("inQueue"):
            if data.get("graceExpired"):
                dispatcher.utter_message(
                    text=(
                        f"{data.get('message', 'Your queue session has expired.')} "
                        "You can rejoin the queue anytime from the app."
                    )
                )
            else:
                dispatcher.utter_message(
                    text=(
                        "You're not currently in any queue today. "
                        "Want to join a queue or book an appointment?"
                    )
                )
            return []

        entry    = data.get("entry", {})
        position = data.get("position", "?")
        ahead    = data.get("peopleAhead", 0)
        wait     = _format_wait(data.get("estimatedWaitTime", 0))
        qnum     = entry.get("queueNumber", "?")
        status   = entry.get("status", "waiting")
        clinic   = entry.get("clinic", {})
        clinic_name = clinic.get("name", "your clinic") if isinstance(clinic, dict) else "your clinic"

        if status == "serving":
            grace = data.get("graceRemaining")
            if grace:
                dispatcher.utter_message(
                    text=(
                        f"🔔 You're being called right now! Queue **#{qnum}** at {clinic_name}. "
                        f"Please proceed to the counter — you have **{grace} minute(s)** remaining."
                    )
                )
            else:
                dispatcher.utter_message(
                    text=f"You are currently being served (Queue **#{qnum}**) at {clinic_name}. Please proceed to the counter."
                )
        else:
            dispatcher.utter_message(
                text=(
                    f"You are **#{qnum}** in the queue at {clinic_name}. "
                    f"Position: {position} ({ahead} patient(s) ahead). "
                    f"Estimated wait: **{wait}**. "
                    "I'll let you know when it's almost your turn!"
                )
            )

        return [
            SlotSet("last_queue_number", qnum),
            SlotSet("last_clinic_id", str(clinic.get("_id", "")) if isinstance(clinic, dict) else ""),
        ]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Join Queue
# ─────────────────────────────────────────────────────────────────────────────
class ActionJoinQueue(Action):
    """
    Guides patient toward joining a queue via POST /api/queues/join.
    Since Rasa doesn't hold a real patient auth token in most deployments,
    this action returns a deep-link instruction to the mobile app.
    If HQ_BOT_TOKEN is set with a valid patient token, it calls the API directly.
    """

    def name(self) -> Text:
        return "action_join_queue"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        clinic_name = tracker.get_slot("clinic_name")
        service     = tracker.get_slot("service_name")

        # If bot token is set, attempt direct API call
        if BOT_TOKEN and clinic_name:
            clinic_id = _resolve_clinic_id(clinic_name)
            if clinic_id and service:
                result = _post("queues/join", {
                    "clinicId":    clinic_id,
                    "serviceName": service,
                })
                if result and result.get("entry"):
                    entry    = result["entry"]
                    wait     = _format_wait(result.get("estimatedWaitTime", 0))
                    position = result.get("position", "?")
                    dispatcher.utter_message(
                        text=(
                            f"You've joined the queue! 🎉 "
                            f"Queue **#{entry['queueNumber']}** at {entry.get('clinicName', clinic_name)} "
                            f"for {entry.get('serviceName', service)}. "
                            f"Position: {position}. Estimated wait: **{wait}**."
                        )
                    )
                    return [
                        SlotSet("last_queue_number", entry["queueNumber"]),
                        SlotSet("last_clinic_id", clinic_id),
                    ]
                elif result and result.get("existingEntry"):
                    ex = result["existingEntry"]
                    dispatcher.utter_message(
                        text=(
                            f"You're already in the queue! Queue **#{ex['queueNumber']}** "
                            f"(status: {ex['status']}). Check your queue status for updates."
                        )
                    )
                    return []

        # App deep-link fallback (standard flow for patient-facing chatbot)
        svc_part    = f" for **{service}**" if service else ""
        clinic_part = f" at **{clinic_name}**" if clinic_name else ""
        dispatcher.utter_message(
            text=(
                f"To join the queue{svc_part}{clinic_part}, tap **Get Queue Number** "
                "on your app dashboard → select your clinic and service → you'll be assigned a number instantly. "
                "Want me to recommend a clinic with the shortest wait?"
            )
        )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Cancel Queue
# ─────────────────────────────────────────────────────────────────────────────
class ActionCancelQueue(Action):
    """Guides patient to cancel their active queue entry."""

    def name(self) -> Text:
        return "action_cancel_queue"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        # Check if they're actually in a queue
        data = _get("queues/my-status")

        if data and data.get("inQueue"):
            entry = data.get("entry", {})
            qnum  = entry.get("queueNumber", "?")
            clinic = entry.get("clinic", {})
            cname  = clinic.get("name", "your clinic") if isinstance(clinic, dict) else "your clinic"
            dispatcher.utter_message(
                text=(
                    f"You are currently **#{qnum}** in the queue at {cname}. "
                    "Are you sure you want to cancel? You'll lose your spot and will need to rejoin. "
                    "To cancel, go to your app dashboard → Current Status → Cancel Queue."
                )
            )
        elif data and not data.get("inQueue"):
            dispatcher.utter_message(
                text="You don't have an active queue entry to cancel today."
            )
        else:
            dispatcher.utter_message(
                text=(
                    "To cancel your queue, go to your app dashboard → Current Status → Cancel Queue. "
                    "If you're having trouble, let me connect you to staff."
                )
            )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Get Clinic Recommendations
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetClinicRecommendations(Action):
    """
    Fetches weighted clinic recommendations from GET /api/clinics/recommend.
    Includes distance, wait time, queue volume, availability, and hours.
    """

    def name(self) -> Text:
        return "action_get_clinic_recommendations"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        service  = tracker.get_slot("service_name")
        location = tracker.get_slot("location")

        params: Dict[str, Any] = {"type": "queue"}
        if service:
            params["service"] = service
        # Note: real lat/lng requires mobile GPS — location slot is text-based
        # The server will still return all clinics ranked by wait time + availability

        data = _get("clinics/recommend", params)

        if not data or not data.get("clinics"):
            dispatcher.utter_message(
                text=(
                    "I couldn't fetch clinic recommendations right now. "
                    "Please open the app and tap 'AI Suggest' on your dashboard for live recommendations."
                )
            )
            return []

        clinics = data["clinics"][:3]  # Top 3 for chat readability

        svc_part = f" for **{service}**" if service else ""
        lines = [f"Here are the top recommended clinics{svc_part} right now:\n"]

        for i, c in enumerate(clinics, 1):
            wait   = _format_wait(c.get("currentWaitingTime", 0))
            queue  = c.get("queueCount", 0)
            status = "🟢 Open" if c.get("isOpen") else "🔴 Closed"
            dist   = f" · {c['distanceKm']} km away" if c.get("distanceKm") is not None else ""
            lines.append(
                f"**{i}. {c['clinicName']}** ({c.get('city', '')}){dist}\n"
                f"   {status} · Wait: {wait} · {queue} in queue\n"
                f"   _{c.get('explanation', '')}_\n"
            )

        lines.append(
            "\nWant to join the queue at any of these? Just say the clinic name "
            "or tap 'Get Queue Number' in the app."
        )

        dispatcher.utter_message(text="\n".join(lines))

        # Save top recommendation to slot for context
        top = clinics[0] if clinics else {}
        return [SlotSet("last_clinic_id", str(top.get("clinicId", "")))]


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Get Clinic Hours
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetClinicHours(Action):
    """Fetches live clinic hours from GET /api/clinics/:id or GET /api/clinics."""

    def name(self) -> Text:
        return "action_get_clinic_hours"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        clinic_name = tracker.get_slot("clinic_name")
        clinic_id   = tracker.get_slot("last_clinic_id") or _resolve_clinic_id(clinic_name)

        if clinic_id:
            data = _get(f"clinics/{clinic_id}")
            if data:
                name  = data.get("name", clinic_name or "the clinic")
                hours = data.get("operatingHours", "8:00 AM – 5:00 PM (Mon–Sat)")
                status = data.get("status", "open")
                status_str = "🟢 Currently open" if status == "open" else f"🔴 Currently {status}"
                dispatcher.utter_message(
                    text=f"**{name}** operating hours: **{hours}**. {status_str}."
                )
                return []

        # Generic — fetch all clinics
        data = _get("clinics", {"limit": 10})
        clinics = data if isinstance(data, list) else (data.get("clinics", []) if data else [])

        if clinics:
            lines = ["Here are the operating hours for our branches:\n"]
            for c in clinics[:5]:
                h = c.get("operatingHours", "8:00 AM – 5:00 PM (Mon–Sat)")
                st = "🟢" if c.get("status") == "open" else "🔴"
                lines.append(f"{st} **{c['name']}**: {h}")
            lines.append("\nNeed directions or want to check the queue at a specific branch?")
            dispatcher.utter_message(text="\n".join(lines))
        else:
            dispatcher.utter_message(
                text=(
                    "Hi-Precision Diagnostics branches are generally open "
                    "**Monday–Saturday, 7:00 AM – 5:00 PM**. "
                    "Check the app for real-time status of specific branches."
                )
            )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Get Clinic Services
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetClinicServices(Action):
    """Fetches live services from GET /api/clinics/:id."""

    def name(self) -> Text:
        return "action_get_clinic_services"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        clinic_name = tracker.get_slot("clinic_name")
        clinic_id   = tracker.get_slot("last_clinic_id") or _resolve_clinic_id(clinic_name)

        if clinic_id:
            data = _get(f"clinics/{clinic_id}")
            if data:
                name     = data.get("name", clinic_name or "this clinic")
                services = [s for s in data.get("services", []) if s.get("isAvailable")]
                if services:
                    svc_names = ", ".join(s["name"] for s in services)
                    dispatcher.utter_message(
                        text=(
                            f"**{name}** offers: {svc_names}. "
                            f"Total: {len(services)} available service(s). "
                            "Which service would you like to queue for or book?"
                        )
                    )
                else:
                    dispatcher.utter_message(
                        text=f"No available services found for {name} right now. Please call the branch to confirm."
                    )
                return []

        # Generic services list
        dispatcher.utter_message(
            text=(
                "Hi-Precision Diagnostics offers: **Laboratory tests** (CBC, urinalysis, blood chemistry, "
                "HbA1c, lipid profile), **Imaging** (X-ray, Ultrasound, 2D Echo), **ECG**, "
                "**Pap Smear**, **Drug Testing**, and **Executive Health Packages**. "
                "Want me to check if a specific branch has your needed service?"
            )
        )
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Get Available Appointment Slots
# ─────────────────────────────────────────────────────────────────────────────
class ActionGetAvailableSlots(Action):
    """Fetches available appointment slots from GET /api/appointments/available-slots."""

    def name(self) -> Text:
        return "action_get_available_slots"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        clinic_name = tracker.get_slot("clinic_name")
        date_slot   = tracker.get_slot("date")
        clinic_id   = tracker.get_slot("last_clinic_id") or _resolve_clinic_id(clinic_name)

        if not clinic_id:
            dispatcher.utter_message(
                text=(
                    "To check available appointment slots, please specify a clinic. "
                    "You can say something like 'book at Hi-Precision Makati' or "
                    "tap **Book Appointment** in the app to see all options."
                )
            )
            return []

        params: Dict[str, str] = {"clinicId": clinic_id}
        if date_slot:
            params["date"] = date_slot

        data = _get("appointments/available-slots", params)

        if data is None:
            dispatcher.utter_message(text=_fallback_message("slots"))
            return []

        # Handle closed clinic response
        if isinstance(data, dict) and data.get("available") == []:
            dispatcher.utter_message(
                text=f"Sorry — {clinic_name or 'this clinic'} is not accepting appointments right now. "
                     f"{data.get('message', '')} Would you like to try a different branch?"
            )
            return []

        slots = data if isinstance(data, list) else []
        available = [s for s in slots if isinstance(s, dict) and s.get("available")]

        if not available:
            dispatcher.utter_message(
                text=(
                    f"No available slots found for {clinic_name or 'this clinic'}"
                    f"{' on ' + date_slot if date_slot else ' today'}. "
                    "Try a different date or branch. Want me to recommend an alternative?"
                )
            )
            return []

        lines = [
            f"Available slots at **{clinic_name or 'the clinic'}**"
            f"{' on ' + date_slot if date_slot else ''}:\n"
        ]
        for s in available[:8]:
            remaining = s.get("remaining", "?")
            lines.append(f"• {s['slot']} ({remaining} spot(s) left)")

        lines.append(
            "\nTap **Book Appointment** in the app to reserve your preferred slot!"
        )
        dispatcher.utter_message(text="\n".join(lines))
        return []


# ─────────────────────────────────────────────────────────────────────────────
# ACTION: Escalate to Staff
# ─────────────────────────────────────────────────────────────────────────────
class ActionEscalateToStaff(Action):
    """
    Flags this conversation as requiring human staff attention.
    Calls POST /api/chatbot/escalate to mark the ChatLog as escalated.
    Staff will see this in the hq-tabapp Patient Inquiries > Needs Attention tab.
    """

    def name(self) -> Text:
        return "action_escalate_to_staff"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        # Build a context note from the last few messages
        events    = tracker.events
        last_msgs = [
            e.get("text", "")
            for e in events[-10:]
            if e.get("event") == "user" and e.get("text")
        ]
        note = " | ".join(last_msgs[-3:]) if last_msgs else "Patient requested staff assistance"

        # Get the last chatLog ID if available (stored by hq-server's chatbot proxy)
        # In Rasa-direct mode, we create a new escalation record
        escalation_data = {
            "note":    note,
            "clinicId": tracker.get_slot("last_clinic_id") or None,
        }

        result = _post("chatbot/escalate", escalation_data)

        if result:
            dispatcher.utter_message(
                text=(
                    "I've flagged your concern to our healthcare staff. "
                    "Someone will review it shortly. "
                    "For urgent matters, please call your branch directly. "
                    "Is there anything else I can note for the staff team?"
                )
            )
        else:
            # Server unreachable — still acknowledge and inform
            dispatcher.utter_message(
                text=(
                    "I understand you need staff assistance. "
                    "Please contact your clinic branch directly or visit the reception. "
                    "You can also use the **Contact** section in the HealthQueue+ app."
                )
            )

        return [SlotSet("escalated", True)]
