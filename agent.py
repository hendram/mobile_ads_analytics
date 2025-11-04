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
5. Wait for the agent's response and return the lat/lng to the user, remember this for later use.
6. After get lat/lng, address from user input them to firestoreetasearch to get data which carId is it and existing ETA in this
   structure argument:
    {lat: "lat", lng: "lng", destination_address: "destination_address from user" }
   remember this for later use, 
   if not exist yet tell user that car never run at all and finish it. 
7. After get carId and lat/lng, then input into firestoretimestampsearch to know if there are leading or lagging between 
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
        "and leg.lat/lng exactly match input lat/lng. Returns carId and eta."
    )

    async def _run_async_impl(self, ctx):
        import json
        from google.cloud import firestore

        db = firestore.Client()

        # --- Parse JSON input ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text

        if not raw:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": "JSON input required: {lat, lng, destination_address}"
                    }))
                ])
            )
            return

        try:
            payload = json.loads(raw)
            lat_in = float(payload.get("lat"))
            lng_in = float(payload.get("lng"))
            dest = (payload.get("destination_address") or "").strip().lower()
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

        if not dest:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": "destination_address is required"
                    }))
                ])
            )
            return

        try:
            # --- Scan top-level collections ---
            for col in db.collections():
                cname = col.id
                if "_coll_" not in cname:
                    continue

                carId = cname.split("_coll_", 1)[0]

                # Iterate trip docs inside this collection
                for trip_doc in col.stream():
                    for leg_coll in trip_doc.reference.collections():
                        if not leg_coll.id.startswith("leg_"):
                            continue

                        for leg_doc in leg_coll.stream():
                            leg = leg_doc.to_dict() or {}

                            to_field = (leg.get("to") or "").lower()
                            if dest not in to_field:
                                continue

                            leg_lat = leg.get("lat")
                            leg_lng = leg.get("lng")

                            if leg_lat == lat_in and leg_lng == lng_in:
                                # Found exact match
                                yield Event(
                                    author=self.name,
                                    content=types.Content(parts=[
                                        types.Part(text=json.dumps({
                                            "carId": carId,
                                            "eta": leg.get("eta")
                                        }))
                                    ])
                                )
                                return

            # --- No match found ---
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "message": "No matching ETA found for given lat/lng and destination."
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


# -------------------------
# LLM Agent: Semantic Processor
# -------------------------
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[directions_tool, firestoreetasearch_tool]
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
