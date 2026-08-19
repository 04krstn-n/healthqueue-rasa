"""
DomainKeywordGate — a custom Rasa NLU pipeline component.

Instead of trying to enumerate every possible OUT-of-domain phrase (an
unbounded set), this checks for the PRESENCE of known HealthQueue+ domain
vocabulary (a small, finite, easy-to-maintain set). If none of it shows up
in the message, and the message isn't a safe universal intent (greet,
goodbye, affirm, deny, bot_challenge), the intent is forced to
`out_of_scope` with full confidence — overriding whatever DIET guessed.

This is a SAFETY NET layered on top of DIETClassifier + FallbackClassifier,
not a replacement for them. DIET/Fallback still handle genuinely ambiguous
in-domain phrasing (e.g. ask_requirements vs ask_payment). This gate only
fires when there is NO domain anchor word present at all — which is the
"paano magluto ng adobo" class of failure.

Place this file at the project root (next to actions.py) and register it
in config.yml as:

    - name: domain_gate.DomainKeywordGate

right after DIETClassifier + EntitySynonymMapper, and before FallbackClassifier.
"""

from __future__ import annotations
from typing import Any, Dict, List, Text

from rasa.engine.graph import ExecutionContext, GraphComponent
from rasa.engine.recipes.default_recipe import DefaultV1Recipe
from rasa.engine.storage.resource import Resource
from rasa.engine.storage.storage import ModelStorage
from rasa.shared.nlu.training_data.message import Message
from rasa.shared.nlu.constants import INTENT, INTENT_RANKING_KEY

# ── Domain vocabulary allowlist ──────────────────────────────────────────
# Keep this list to CONTENT words that clearly signal a HealthQueue+ topic.
# Don't add filler/function words (ba, po, ako, ang, ng, ...) — they appear
# in almost every Tagalog sentence and would defeat the purpose of the gate.
#
# Maintenance rule: only add to this list when a REAL in-domain user message
# gets wrongly rejected because it didn't contain a recognized keyword.
# Don't add to it speculatively — that's the same infinite-list trap, just
# on the allow side instead of the deny side (though a miss here is a much
# safer failure than a miss on a denylist: worst case is a legitimate
# question gets a polite "that's outside my scope, but I can help with..."
# instead of a wrong answer).

DOMAIN_KEYWORDS = {
    # queue
    "queue", "pila", "line", "linya", "number", "numero",
    # wait / time
    "wait", "waiting", "hintay", "tagal", "matagal", "eta", "minuto", "minute",
    # clinic / branch
    "clinic", "klinika", "branch", "sangay", "hospital", "ospital",
    "hi-precision", "hiprecision", "healthqueue", "hq",
    # location
    "location", "address", "malapit", "nearest", "near", "location", "map",
    "saan", "san", "address", "direksyon", "directions",
    # hours
    "hours", "oras", "bukas", "sara", "sarado", "open", "close", "closed", "schedule",
    # appointment
    "appointment", "appt", "book", "booking", "reserve", "reservation",
    "reschedule", "resched", "cancel", "cancelled",
    # services / tests
    "service", "services", "serbisyo", "test", "lab", "laboratory", "cbc",
    "xray", "x-ray", "ultrasound", "ecg", "echo", "urinalysis", "fbs",
    "checkup", "check-up", "screening", "panel",
    # requirements
    "requirement", "requirements", "kailangan", "kelangan", "klngan",
    "fasting", "fast", "prescription", "referral", "id", "prepare", "ihanda", "dalhin",
    # payment
    "payment", "bayad", "presyo", "price", "magkano", "gcash", "maya",
    "cash", "card", "credit", "debit", "fee", "fees", "cost",
    # philhealth / hmo
    "philhealth", "hmo", "maxicare", "medicard", "intellicare", "philcare",
    "insurance", "coverage", "loa", "mdr",
    # results
    "result", "results", "release", "releasing",
    # staff / complaint
    "staff", "agent", "human", "person", "tao", "complaint", "reklamo",
    "problem", "issue", "sumbong",
    # queue actions
    "join", "sumali", "pumila", "status",
}

ALWAYS_ALLOWED_INTENTS = {"greet", "goodbye", "affirm", "deny", "bot_challenge", "nlu_fallback"}


@DefaultV1Recipe.register(
    DefaultV1Recipe.ComponentType.INTENT_CLASSIFIER, is_trainable=False
)
class DomainKeywordGate(GraphComponent):
    """Forces out-of-scope for messages with zero domain vocabulary."""

    @classmethod
    def create(
        cls,
        config: Dict[Text, Any],
        model_storage: ModelStorage,
        resource: Resource,
        execution_context: ExecutionContext,
    ) -> "DomainKeywordGate":
        return cls(config)

    def __init__(self, config: Dict[Text, Any]) -> None:
        self.component_config = config

    @classmethod
    def get_default_config(cls) -> Dict[Text, Any]:
        return {}

    def process(self, messages: List[Message]) -> List[Message]:
        for message in messages:
            text = (message.get("text") or "").lower()
            current_intent = message.get(INTENT, {}) or {}
            intent_name = current_intent.get("name")

            if intent_name in ALWAYS_ALLOWED_INTENTS:
                continue

            has_domain_word = any(kw in text for kw in DOMAIN_KEYWORDS)

            if not has_domain_word:
                forced = {"name": "out_of_scope", "confidence": 1.0}
                message.set(INTENT, forced, add_to_output=True)
                ranking = message.get(INTENT_RANKING_KEY, []) or []
                message.set(
                    INTENT_RANKING_KEY,
                    [forced] + [r for r in ranking if r.get("name") != "out_of_scope"],
                    add_to_output=True,
                )

        return messages