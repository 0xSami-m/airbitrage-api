#!/usr/bin/env python3
"""Dry-run wrapper for book_virgin_atlantic.py"""
import os, sys, asyncio
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["DRY_RUN"] = "true"

from book_virgin_atlantic import book_virgin_atlantic

result = asyncio.run(book_virgin_atlantic(
    origin="LHR", dest="JFK", date="2026-04-17",
    first="Sami", last="Muduroglu", dob="2003-04-23",
    cabin="economy", card_last4="2002", booking_id=None,
))
print(f"Result: {result}")
