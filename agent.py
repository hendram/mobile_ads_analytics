# agent.py — ADK-compatible multi-agent pipeline
import os
import json
import logging
from pydantic import BaseModel
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


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/mobile_ads_analytics/serviceAccountKey.json"

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
You are a geographic assistant. Your task is to convert a human-readable address into latitude and longitude.

Rules:
1. Always extract the full address from the user input.
   - Example: "Please give lat, lng from McDonald's Cipayung Jl. Cipayung Raya, Jakarta 13840 Indonesia"
     → you must extract: "McDonald's Cipayung Jl. Cipayung Raya, Jakarta 13840 Indonesia"
2. If the user input is vague or incomplete, ask for a complete address.
3. Always respond by calling the DirectionsAgent tool with structured argument:
{
"destination_address": "<full_address>"
}
4. Only call the DirectionsAgent for geocoding (do not attempt analysis or other tasks).
5. Wait for the agent's response and return the lat/lng to the user if asked, remember this for later use.
6. Next fed address from user to firestoreetasearch to get data which carId is it and existing ETA in this
   structure argument:
    { destination_address: "destination_address from user" }
   remember this for later use, and return ETA to user if asked 
   if not exist yet tell user that car never run at all and finish it. 
7.  Next fed carId and lat/lng as input into firestoretimestampsearch to get timestamp. If there are leading or lagging between 
   eta prediction and timestamp at that location in this structure argument:
    {id: "carId", lat: "lat", lng: "lng" }
    Keep timestamp result for later use, and comparing ETA data with real
   timestamp get from firestoretimestampsearch, if there are leading or lagging tell user how much time by hour, minutes, and
   seconds. 
   If not exist yet tell user that car still not reach that destination, and finish it. 
8. And next offering user if they  want to adjust their video on dashboard and on mobile car to match next destination which 
   will be pass on by car as video viewed must be not sync with current condition as ETA and timestamp on this position already
   mismatch. 
9. If yes, then run tools adjustvideo and input hous, minutes, and seconds in format timestamp as expectedTime variable 
   and send it to http://localhost:3001/adksend with this data structure 
   { carId, expectedTime } 
"""

# -------------------------
# BaseAgents (specialized agents)
# -------------------------
DIRECTION_API = os.getenv("DIRECTION_API")

executor = ThreadPoolExecutor(max_workers=2)

import requests, os, json

def geocode_address_sync(address: str):
    """Call Google Maps Geocoding API synchronously."""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": DIRECTION_API}
    resp = requests.get(url, params=params, timeout=10)
    data = resp.json()

    # DEBUG: log the full API response
    logger.info("Geocoding API response: %s", json.dumps(data, indent=2))

    # ✅ Handle result extraction properly
    results = data.get("results", [])
    if not results:
        return None

    # ✅ Prefer a result that has location_type == "ROOFTOP"
    best = next((r for r in results if r["geometry"]["location_type"] == "ROOFTOP"), results[0])
    loc = best["geometry"]["location"]
    return (loc["lat"], loc["lng"])

# -------------------------
# Firestore client setup
# -------------------------
db = firestore.Client()  # Make sure GOOGLE_APPLICATION_CREDENTIALS is set

class DirectionsAgent(BaseAgent):
    name: str = "DirectionsAgent"
    description: str = "Resolves address to lat/lng."

    async def _run_async_impl(self, ctx):
        # Support dict input
        address = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            address = ctx.user_content.parts[0].text
        # If user_input is dict, extract destination_address
      
        if not address:
            part = types.Part(text=json.dumps({"error": "No address provided"}))
            yield Event(author=self.name, content=types.Content(parts=[part]))
            return

         # Call Google Maps geocoding API in thread
        loop = asyncio.get_running_loop()
        lat_lng = await loop.run_in_executor(None, geocode_address_sync, address)
         
        if not lat_lng:
            part = types.Part(text=json.dumps({"error": "Could not geocode address"}))
            yield Event(author=self.name, content=types.Content(parts=[part]))
            return

        # Successful result
        result = {"address": address, "lat_lng": lat_lng}
        print("result lat lng", result)
        part = types.Part(text=json.dumps(result))
        yield Event(author=self.name, content=types.Content(parts=[part]))


class FirestoreEtaSearchAgent(BaseAgent):
    name: str = "FirestoreEtaSearchAgent"
    description: str = (
        "Search <carId>_coll_* collections for ETA where leg 'to' matches destination_address "
        "in the newest collection per carId. Returns carId and eta."
    )

    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Get input string ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text
            print("raw input:", raw)

        if not raw:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": "JSON input required: {destination_address}"
                    }))
                ])
            )
            return

        try:
            # --- Group collections by carId ---
            car_collections = defaultdict(list)
            for col in db.collections():
                cname = col.id
                print("found collection:", cname)
                if "_coll_" not in cname:
                    continue
                try:
                    carId, timestamp = cname.split("_coll_", 1)
                except ValueError:
                    continue
                car_collections[carId].append((timestamp, col))

            # --- Pick newest collection per carId ---
            newest_collections = {}
            for carId, cols in car_collections.items():
                newest_timestamp, newest_col = max(cols, key=lambda x: x[0])
                newest_collections[carId] = newest_col
                print(f"carId {carId}, newest collection {newest_col.id}")

            # --- Iterate legs in newest collection and find matching ETA ---
            for carId, col in newest_collections.items():
                for leg_doc in col.stream():  # leg_1, leg_2, etc.
                    leg = leg_doc.to_dict() or {}
                    print("leg_doc id:", leg_doc.id)
                    print("leg fields:", leg)
                    to_field = leg.get("to", "")
                    print("to_field:", to_field)
                    if raw in to_field:
                        eta_event = Event(
                            author=self.name,
                            content=types.Content(parts=[
                                types.Part(text=json.dumps({
                                    "carId": carId,
                                    "eta": leg.get("eta")
                                }))
                            ])
                        )
                        print("Yielding ETA event:", eta_event)
                        yield eta_event
                        return  # stop after first match

            # --- No match found ---
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "message": "No matching ETA found for given destination_address."
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



class FirestoreTimestampSearchAgent(BaseAgent):
    name: str = "FirestoreTimestampSearchAgent"
    description: str = (
        "Search cars_latest_position for newest document per carId, "
        "then search in subcollection 'positions' for input lat/lng. "
        "Returns carId and timestamp."
    )

    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Parse JSON input ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text
            print("raw input:", raw)

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
            carId_input = payload.get("carId", "").strip()
            lat_input = float(payload.get("lat", 0))
            lng_input = float(payload.get("lng", 0))
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
                lat = pos.get("lat")
                lng = pos.get("lng")
                print(f"Checking position: {lat}, {lng}")
                if lat == lat_input and lng == lng_input:
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
directions_tool = agent_tool.AgentTool(agent=DirectionsAgent())
firestoreetasearch_tool = agent_tool.AgentTool(agent=FirestoreEtaSearchAgent())
firestoretimestampsearch_tool = agent_tool.AgentTool(agent=FirestoreTimestampSearchAgent())

# -------------------------
# LLM Agent: Semantic Processor
# -------------------------
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[directions_tool, firestoreetasearch_tool, firestoretimestampsearch_tool]
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
