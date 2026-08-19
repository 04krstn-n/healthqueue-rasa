# hq-rasa — HealthQueue+ Rasa Chatbot

NLU-powered chatbot for the HealthQueue+ patient mobile app.
Handles: booking, queue status, clinic hours, services, payments, PhilHealth/HMO, and more.

## Requirements
- Python 3.10.x (exact — Rasa is strict about versions)
- pip

## Setup

```bash
cd hq-rasa

# 1. Create virtual environment
py -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Train the model
rasa train

# 4. Run the Rasa server (REST webhook)
rasa run --enable-api --cors "*" --port 5005

# 5. (Optional) Run custom action server in a separate terminal
rasa run actions --port 5055
```

## Connecting to hq-server

In `hq-server/.env`, set:
```
RASA_SERVER_URL=http://localhost:5005
```

The chatbot priority chain becomes:
1. **Rasa** — handles all trained intents (book, queue, hours, etc.)
2. **OpenAI GPT-4o-mini** — handles anything Rasa doesn't cover
3. **FAQ keyword match** — offline fallback

## Intents covered
| Intent | Examples |
|--------|----------|
| `book_appointment` | "I want to book an appointment", "mag-book ng appointment" |
| `check_queue_status` | "what's my queue number", "pila status" |
| `get_wait_time` | "how long is the wait", "matagal ba ang pila" |
| `get_queue_number` | "get queue number", "kumuha ng number" |
| `get_clinic_hours` | "when is the clinic open", "anong oras bukas" |
| `get_clinic_location` | "where is the clinic", "saan ang clinic" |
| `get_clinic_services` | "what tests are available", "do you have x-ray" |
| `cancel_appointment` | "cancel my appointment" |
| `reschedule_appointment` | "reschedule my appointment" |
| `ask_requirements` | "what do I need to bring", "fasting required" |
| `ask_payment` | "do you accept GCash", "payment methods" |
| `ask_results` | "when will my results be ready" |
| `ask_philhealth` | "do you accept PhilHealth" |
| `ask_hmo` | "do you accept Maxicare" |

## Custom Actions (optional)
`actions/actions.py` contains two live actions that call hq-server:
- `ActionGetLiveWaitTime` — fetches real-time wait time per clinic
- `ActionGetClinicHours` — fetches clinic schedule from DB

To use these, set `HQ_SERVER_URL=http://localhost:4000/api` in your environment and run:
```bash
rasa run actions --port 5055
```

## Retraining
After editing `data/nlu.yml`, `data/stories.yml`, or `domain.yml`:
```bash
rasa train
rasa run --enable-api --cors "*" --port 5005
```
