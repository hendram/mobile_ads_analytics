# agent.py — ADK-compatible multi-agent pipeline
import os
import json
import logging
from google.adk.agents import LlmAgent, BaseAgent
from google.adk.tools import agent_tool
from google.adk.events import Event
from google.genai import types
from mobile_ads_analytics.tools import save_json, run_query
from concurrent.futures import ThreadPoolExecutor
import requests
import asyncio
import sys
from google.cloud import firestore
import math
import re
from collections import defaultdict
from google.cloud import firestore


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/agents/mobile_ads_analytics/serviceAccountKey.json"

logger = logging.getLogger("directions_agent")
logger.setLevel(logging.INFO)

# Add a StreamHandler to stdout if none exist
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

DIRECTIONS_AGENT_INSTRUCTION = """
You are a mobile ads analytics assistant. Your task is to get all requried data, calculated based on user input, and present it back to user. 
Choose rules based on match scenario.

Rules:
1. Scenario 1: Fuel Cost
   - If user asks something like "Please calculate all fuel cost for car1 if predictive cost per meters is 1 USD", you must:
     a. Run the tool `firestoredistanceanalytics` with input in format: carId without spaces (e.g., car1).
     b. Get all legs returned from Firestore (fields: from, to, distance).
     c. Calculate fuel cost for each leg using formula: cost = distance * X USD (X is user-provided cost per meter or per km; convert if needed).
     d. Present a table showing: distance, from, to, fuel cost.
     e. If JSON is empty, incomplete, or tool failed, respond clearly: "No data received or missing fields".

2. Scenario 2 For example user asked for "Please calculate all professional driver cost for car2 if predictive cost per 10 meters is 1 USD", 
   then you can run tools firestoredistanceanalytics to get data required by giving input in format:
   car2 without space at all in between 
   and get all distance, from and to, then calculate based on each distance by using formula cost = distance/10 * 1 USD, then present back to user
   as table listing field distance, from, to, professional driver cost.
"""

# -------------------------
# BaseAgents (specialized agents)
# -------------------------
DIRECTION_API = os.getenv("DIRECTION_API")

executor = ThreadPoolExecutor(max_workers=2)

import requests, os, json

class FirestoreDistanceAnalyticsAgent(BaseAgent):
    name: str = "FirestoreDistanceAnalyticsAgent"
    description: str = (
        "For the given carId, find the newest collection <carId>_coll_* "
        "and return all legs with their from, to, and distance."
    )

    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Get input string (carId) ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text.strip()
            print("raw input:", raw)
 
        if not raw:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": "JSON input required: {carId}"
                    }))
                ])
            )
            return

        carId = raw
        try:
            # --- Find all collections that match this carId ---
            collections = []
            for col in db.collections():
                cname = col.id
                if cname.startswith(f"{carId}_coll_"):
                    try:
                        _, timestamp = cname.split("_coll_", 1)
                        collections.append((timestamp, col))
                    except ValueError:
                        continue

            if not collections:
                yield Event(
                    author=self.name,
                    content=types.Content(parts=[
                        types.Part(text=json.dumps({
                            "message": f"No collections found for carId {carId}."
                        }))
                    ])
                )
                return

            # --- Pick newest collection ---
            newest_timestamp, newest_col = max(collections, key=lambda x: x[0])
            print(f"Newest collection for {carId}: {newest_col.id}")

            # --- Collect all legs ---
            results = []
            legs = list(newest_col.stream())
            print("Total legs found:", len(legs))

            results = []
            for i, leg_doc in enumerate(legs):
                leg = leg_doc.to_dict() or {}
                print(f"Leg #{i+1}:", leg)
                entry = {
                    "carId": carId,
                    "from": leg.get("from", ""),
                    "to": leg.get("to", ""),
                    "distance": leg.get("distance", 0)
                }
                results.append(entry)
            print(f"After append #{i+1}: total results = {len(results)}")

            print("Total results collected:", len(results))
            # --- Return all legs ---
            event = Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps(results, indent=2))
                ])
            )

            print("event", event)
            yield event

            
        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Firestore error: {str(e)}"
                    }))
                ])
            )


class FirestoreTimestampSearchAgent(BaseAgent):
    name: str = "FirestoreTimestampSearchAgent"
    description: str = (
        "Search cars_position for newest document per carId, "
        "then search in subcollection 'positions', subdocuments for input lat/lng."
        "Returns carId and timestamp."
    )

    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Parse JSON input ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text
            print("raw input:", raw, type(raw))

        if not raw:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": "JSON input required: {carId, lat, lng}"
                    }))
                ])
            )
            return

        try:
            payload = json.loads(raw)
            carId_input = payload.get('carId', "").strip()
            print("carId_input", carId_input)
            lat_input = float(payload.get('lat', 0))
            lng_input = float(payload.get('lng', 0))
            if not carId_input:
                raise ValueError("carId is required")
        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Invalid JSON or missing fields: {str(e)}"
                    }))
                ])
            )
            return

        try:
            # --- Find all docs with prefix carId_ in cars_latest_position ---
            latest_doc = None
            latest_timestamp = None
            for doc in db.collection("cars_latest_position").stream():
                doc_id = doc.id
                print("doc_id", doc_id)
                if not doc_id.startswith(f"{carId_input}_"):
                    continue
                # get timestamp part
                try:
                    _, timestamp_str = doc_id.split("_", 1)
                except ValueError:
                    continue
                if latest_timestamp is None or timestamp_str > latest_timestamp:
                    latest_timestamp = timestamp_str
                    latest_doc = doc

            if not latest_doc:
                yield Event(
                    author=self.name,
                    content=types.Content(parts=[
                        types.Part(text=json.dumps({
                            "message": f"No documents found for carId {carId_input}"
                        }))
                    ])
                )
                return

            print(f"Newest doc for {carId_input}: {latest_doc.id}")

            # --- Search positions subcollection ---
            positions_coll = latest_doc.reference.collection("positions")
            match_found = False
            for pos_doc in positions_coll.stream():
                pos = pos_doc.to_dict() or {}
                print("pos", pos)
                lat = pos.get("lat")
                lng = pos.get("lng")
                print(f"Checking position: {lat}, {lng}, {lat_input}, {lng_input}")
                tolerance = 0.00001
                if abs(lat - lat_input) < tolerance and abs(lng - lng_input) < tolerance:
                    match_found = True
                    yield Event(
                        author=self.name,
                        content=types.Content(parts=[
                            types.Part(text=json.dumps({
                                "carId": carId_input,
                                "timestamp": latest_timestamp
                            }))
                        ])
                    )
                    break

            if not match_found:
                yield Event(
                    author=self.name,
                    content=types.Content(parts=[
                        types.Part(text=json.dumps({
                            "message": f"No matching position found for carId {carId_input} at given lat/lng"
                        }))
                    ])
                )

        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Firestore error: {str(e)}"
                    }))
                ])
            )

# -------------------------
# Wrap BaseAgents as tools
# -------------------------
firestoredistanceanalytics_tool = agent_tool.AgentTool(agent=FirestoreDistanceAnalyticsAgent())

# -------------------------
# LLM Agent: Semantic Processor
# -------------------------
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[firestoredistanceanalytics_tool],
)


# -------------------------
# LLM Agent: Root / Coordinator
# -------------------------
root_agent = LlmAgent(
    name="RootAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
        "Greet the user if needed. "
        "For analytics queries, delegate to the SemanticAgent tool."
    ),
    tools=[agent_tool.AgentTool(agent=semantic_agent)]
)
