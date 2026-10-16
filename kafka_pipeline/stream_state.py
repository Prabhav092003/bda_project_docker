"""
stream_state.py
Tiny persistent cursor so every "stream next day" request publishes the NEXT trading
day of ticks (previously the same first day was re-sent every time, creating duplicates).
"""

import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def load_state():
    if os.path.exists(config.STREAM_STATE_FILE):
        try:
            with open(config.STREAM_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {'next_day_index': 0, 'total_published': 0, 'last_date': None}


def save_state(state):
    with open(config.STREAM_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)


def reset_state():
    if os.path.exists(config.STREAM_STATE_FILE):
        os.remove(config.STREAM_STATE_FILE)
