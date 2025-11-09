import os
import json
import logging
from google.adk.agents import LlmAgent, BaseAgent, SequentialAgent
from google.adk.tools import agent_tool
from google.adk.events import Event
from google.genai import types
from concurrent.futures import ThreadPoolExecutor
import requests
import sys
from google.cloud import firestore
import re
from collections import defaultdict
from google.cloud import firestore
from datetime import datetime
import math

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/agents/mobile_ads_analytics/serviceAccountKey.json"

logger = logging.getLogger("directions_agent")
logger.setLevel(logging.INFO)

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
     a. Run the tool combined_sequential_agent with input in format: carId without spaces (e.g., car1) and user ask, so make 
        it like json { id: carId, userquery: user asked }

2. Scenario 2: Driver Cost
   - For example user asked for "Please calculate all professional driver cost for car2 if predictive cost per 10 meters is 1 USD", 
   then you can run tools combined_sequential_agent to get data required by giving input in format:
   carId without spaces (e.g., car1) and user ask, so make it like json { id: carId, userquery: user asks }.

3. Scenario 3: Comparing total cost of fuel or driver between 2 car
   - If user ask something like "please calculate total cost of fuel if per 1000 meters will cost 2usd for car 2 and return all waypoint too with table distance, from, to, cost  and 
     comparing with car 1 if cost of fuel for car 1 per 2000 meters will cost 3 usd and make table too with 
     table distance, from, to, cost and calculate different total cost between car 1 and car 2, which one more expensive for total route 
     then you can run tools combined_sequential_agent and  another_seq_agent to input car 1 and car 2 as 2 different json format:
     and intelligently divide user asked for combined_sequential_agent and another_seq_agent based on carId 
     wants to calculate and    
    make it like json { id: carId, userquery: user asked match with carId}  and input it to combined_sequential_agent,
           { id: carId, userquery: user asked match with carId } input it to another_seq_agent which each id different car id and different user asked

4. Scenario 4: Counting how many trip specific car has drove in
     - If user ask like "please give me how many trip this car has drive in ", then you can run tool 
       calculated_trip_carseq by giving input in format:
       carId without spaces (e.g., car1), make it like json { id: carId }

5. Scenario 5: Counting how much elapse time has been since first car drove till last position
     - If user ask like "Please get me elapse time for car1 latest trip", then you can run tools calculated_elapse_timeseq
       to get data by giving input in format:
       carId without spaces (e.g., car1), make it like json { id: carId }
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

        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text.strip()
 
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
        
        raw_json = json.loads(raw)  
        carId = raw_json.get("id")
        user_query = raw_json.get("userquery")
        ctx.session.state["temp:user_query"] = user_query        
        try:
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

            newest_timestamp, newest_col = max(collections, key=lambda x: x[0])

            results = []
            legs = list(newest_col.stream())

            results = []
            for i, leg_doc in enumerate(legs):
                leg = leg_doc.to_dict() or {}
                entry = {
                    "carId": carId,
                    "from": leg.get("from", ""),
                    "to": leg.get("to", ""),
                    "distance": leg.get("distance", 0)
                }
                results.append(entry)

            # --- Return all legs ---
            ctx.session.state["temp:legs"] = results        
     
        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Firestore error: {str(e)}"
                    }))
                ])
            )

class FirestoreDistanceAnalytics2Agent(BaseAgent):
    name: str = "FirestoreDistanceAnalytics2Agent"
    description: str = (
        "For the given carId, find the newest collection <carId>_coll_* "
        "and return all legs with their from, to, and distance."
    )
   
    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text.strip()
 
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
        
        raw_json = json.loads(raw)  
        carId = raw_json.get("id")
        user_query = raw_json.get("userquery")
        ctx.session.state["temp:user_query2"] = user_query        
        try:
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

            newest_timestamp, newest_col = max(collections, key=lambda x: x[0])

            results = []
            legs = list(newest_col.stream())

            results = []
            for i, leg_doc in enumerate(legs):
                leg = leg_doc.to_dict() or {}
                entry = {
                    "carId": carId,
                    "from": leg.get("from", ""),
                    "to": leg.get("to", ""),
                    "distance": leg.get("distance", 0)
                }
                results.append(entry)

            # --- Return all legs ---
            ctx.session.state["temp:legs2"] = results        
     
        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Firestore error: {str(e)}"
                    }))
                ])
            )


Predictivecost_agent = LlmAgent(
    name="PredictivecostAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
      """    Your job is to get user query from {temp:user_query} and calculate all requested with this data {temp:legs}.
        If {temp:legs} is empty or missing, say so clearly.
        Output only the data in JSON or text form, nothing else.
      """ 
    )
)

Predictivecost_agent2 = LlmAgent(
    name="PredictivecostAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
      """    Your job is to get user query from {temp:user_query2} and calculate all requested with this data {temp:legs2}.
        If {temp:legs2} is empty or missing, say so clearly.
        Output only the data in JSON or text form, nothing else.
      """ 
    )
)

class FirestoreCalculatedTripCarAgent(BaseAgent):
    name: str = "FirestoreCalculatedTripCarAgent"
    description: str = (
        "For the given carId, count how many collections <carId>_coll_* exist "
        "to see how many times the car drove that route."
    )
   
    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Get input string (carId) ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text.strip()
 
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
        
        try:
            raw_json = json.loads(raw)
            carId = raw_json.get("id")
            if not carId:
                yield Event(
                    author=self.name,
                    content=types.Content(parts=[
                        types.Part(text=json.dumps({
                            "error": "Missing 'id' in input JSON."
                        }))
                    ])
                )
                return

            # --- Find all collections that match this carId ---
            num_drives = sum(1 for col in db.collections() if col.id.startswith(f"{carId}_coll_"))
            # --- Prepare and yield response ---
            response = {
                "carId": carId,
                "num_drives": num_drives
            }

            # --- Store in session ---
            ctx.session.state[f"temp:num_drives"] = response

        except Exception as e:
            yield Event(
                author=self.name,
                content=types.Content(parts=[
                    types.Part(text=json.dumps({
                        "error": f"Firestore error: {str(e)}"
                    }))
                ])
            )

CalculatedTripCarReport_agent = LlmAgent(
    name="CalculatedTripCarReportAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
      """    Your job is to give return data from {temp:num_drives} .
        If {temp:num_drives} is empty or missing, say so clearly.
        Output only the data in JSON or text form, nothing else.
      """ 
    )
)

def parse_firestore_ts(ts_str: str) -> datetime:
    """
    Parse Firestore timestamp into a consistent, timezone-naive datetime.
    Supports both ISO and compact formats.
    """

    # --- ISO style: 2025-11-06T11:01:05.336Z ---
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z", ts_str):
        ts_norm = ts_str.replace("Z", "")
        dt = datetime.fromisoformat(ts_norm)
        return dt

    # --- Compact Firestore style: 2025-11-06T110100416Z ---
    m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})T(\d{2})(\d{2})(\d{2})(\d+)Z?", ts_str)
    if m:
        date, hh, mm, ss, fsec = m.groups()
        fsec = fsec[:6].ljust(6, "0")  # normalize microseconds
        ts_norm = f"{date}T{hh}:{mm}:{ss}.{fsec}"
        dt = datetime.fromisoformat(ts_norm)
        return dt

    raise ValueError(f"Invalid Firestore timestamp format: {ts_str}")


class FirestoreElapsedTimeAgent(BaseAgent):
    name: str = "FirestoreElapsedTimeAgent"
    description: str = (
        "For a given carId, calculate how many hours, minutes, and seconds "
        "have elapsed since the first drive until the latest drive."
    )

    async def _run_async_impl(self, ctx):
        db = firestore.Client()

        # --- Parse JSON input ---
        raw = None
        if hasattr(ctx, "user_content") and ctx.user_content.parts:
            raw = ctx.user_content.parts[0].text

        if not raw:
             yield Event(
                 author=self.name,
                 content=types.Content(parts=[
                 types.Part(text=json.dumps({"error": "JSON input required: {carId}"}))
                 ])
             )
             return

        try:
            payload = json.loads(raw)
            carId_input = payload.get("id", "").strip()
            if not carId_input:
                raise ValueError("carId is required")
        except Exception as e:
            ctx.session.state["temp:elapsed_time"] = {
                "error": f"Invalid JSON or missing fields: {str(e)}"
            }
            return

        try:
            all_docs = []
            for doc in db.collection("cars_latest_position").stream():
                doc_id = doc.id
                if doc_id.startswith(f"{carId_input}_"):
                    try:
                        _, timestamp_str = doc_id.split("_", 1)
                        all_docs.append((timestamp_str, doc))
                    except ValueError:
                        continue

            if not all_docs:
                ctx.session.state["temp:elapsed_time"] = {
                    "error": f"No documents found for carId {carId_input}"
                }
                return

            all_docs.sort(key=lambda x: x[0])
            latest_timestamp_str, latest_doc = all_docs[-1] 

            latest_dt = parse_firestore_ts(latest_timestamp_str)
            positions_coll = latest_doc.reference.collection("positions")
            latest_position_ts = None
            for pos_doc in positions_coll.stream():
                pos = pos_doc.to_dict() or {}
                ts_str = pos.get("timestamp")  
                if ts_str:
                    pos_dt = parse_firestore_ts(ts_str)
                    if not latest_position_ts or pos_dt > latest_position_ts:
                        latest_position_ts = pos_dt
            if not latest_position_ts:
                latest_position_ts = latest_dt

            elapsed = latest_position_ts - latest_dt
            hours, remainder = divmod(elapsed.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            # --- Store in session ---
            ctx.session.state["temp:elapsed_time"] = {
                "carId": carId_input,
                "elapsed_time": {
                    "hours": int(hours),
                    "minutes": int(minutes),
                    "seconds": int(seconds)
                }
            }

        except Exception as e:
            ctx.session.state["temp:elapsed_time"] = {
                "error": f"Firestore error: {str(e)}"
            }
            return


CalculatedElapseTimeReport_agent = LlmAgent(
    name="CalculatedElapseTimeReportAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
      """    Your job is to give return data from {temp:elapsed_time} .
        If {temp:elapsed_time} is empty or missing, say so clearly.
        Output only the data in JSON or text form, nothing else.
      """ 
    )
)




# -------------------------
# Wrap BaseAgents as tools
# -------------------------
Predictivecost_agent_tool = agent_tool.AgentTool(agent=Predictivecost_agent)
Predictivecost_agent2_tool = agent_tool.AgentTool(agent=Predictivecost_agent2)
firestoredistanceanalytics_tool = agent_tool.AgentTool(agent=FirestoreDistanceAnalyticsAgent())
firestoredistanceanalytics2_tool = agent_tool.AgentTool(agent=FirestoreDistanceAnalytics2Agent())
firestorecalculatedtripcar_tool = agent_tool.AgentTool(agent=FirestoreCalculatedTripCarAgent())
calculatedtripcarreport_tool = agent_tool.AgentTool(agent=CalculatedTripCarReport_agent)
firestoreelapsedtime_tool = agent_tool.AgentTool(agent=FirestoreElapsedTimeAgent())
calculatedelapsetime_tool = agent_tool.AgentTool(agent=CalculatedElapseTimeReport_agent)

another_seq_agent = SequentialAgent(
    name="anotherSequentialAgent",
    sub_agents=[FirestoreDistanceAnalytics2Agent(), Predictivecost_agent2],
    description="Run Firestore fetch first, then process with TestFetch agent using the same session temp."
)


combined_sequential_agent = SequentialAgent(
    name="combinedSequentialAgent",
    sub_agents=[FirestoreDistanceAnalyticsAgent(), Predictivecost_agent],
    description="Run Firestore fetch first, then process with TestFetch agent using the same session temp."
)

calculatedtripcarseq_agent = SequentialAgent(
    name="calculatedtripcarSequentialAgent",
    sub_agents=[FirestoreCalculatedTripCarAgent(), CalculatedTripCarReport_agent],
    description="Run FirestoreCalculatedTripCar first, then process with CalculatedTripCarReport agent using the same session temp."
)

calculatedtripelapsetimeseq_agent = SequentialAgent(
    name="calculatedtripelapsetimeSequentialAgent",
    sub_agents=[FirestoreElapsedTimeAgent(), CalculatedElapseTimeReport_agent],
    description="Run FirestoreElapseTime first, then process with CalculatedElapseTimeReport agent using the same session temp."
)


combined_sequential_agent_tool = agent_tool.AgentTool(agent=combined_sequential_agent)
another_seq_agent_tool = agent_tool.AgentTool(agent=another_seq_agent)
calculated_trip_carseq_tool = agent_tool.AgentTool(agent=calculatedtripcarseq_agent)
calculated_elapse_timeseq_tool = agent_tool.AgentTool(agent=calculatedtripelapsetimeseq_agent)

# Semantic LLM agent uses the sequential agent tool
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[combined_sequential_agent_tool, another_seq_agent_tool, calculated_trip_carseq_tool, calculated_elapse_timeseq_tool]   
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

