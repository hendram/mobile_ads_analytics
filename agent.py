# agent.py — ADK-compatible multi-agent pipeline
import os
import json
import logging
from google.adk.agents import LlmAgent, BaseAgent, SequentialAgent, ParallelAgent
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
     make it like json { id: carId, userquery: user asked }  and input it to combined_sequential_agent,
           { id: carId, userquery: user asked } input it to another_seq_agent which each id different car id 

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
        
        raw_json = json.loads(raw)  # parse string into dict
        carId = raw_json.get("id")
        user_query = raw_json.get("userquery")
        ctx.session.state["temp:user_query"] = user_query        
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
            ctx.session.state["temp:legs"] = results        
            print("SemanticAgent sees temp:legs:", ctx.session.state.get("temp:legs"), ctx.session.state.get("temp:user_query"))
     
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
      """    Your job is to get user query from {temp:user_query} and calculate all requested with this data {temp:legs}.
        If {temp:legs} is empty or missing, say so clearly.
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

another_seq_agent = SequentialAgent(
    name="anotherSequentialAgent",
    sub_agents=[FirestoreDistanceAnalyticsAgent(), Predictivecost_agent2],
    description="Run Firestore fetch first, then process with TestFetch agent using the same session temp."
)


combined_sequential_agent = SequentialAgent(
    name="combinedSequentialAgent",
    sub_agents=[FirestoreDistanceAnalyticsAgent(), Predictivecost_agent],
    description="Run Firestore fetch first, then process with TestFetch agent using the same session temp."
)


combined_sequential_agent_tool = agent_tool.AgentTool(agent=combined_sequential_agent)
another_seq_agent_tool = agent_tool.AgentTool(agent=another_seq_agent)




# -------------------------
# LLM Agent: Semantic Processor
# -------------------------
# Wrap it as a tool for LlmAgent



# Semantic LLM agent uses the sequential agent tool
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[combined_sequential_agent_tool, another_seq_agent_tool]   # ✅ must be a tool, not an agent
)

original_semantic_run = semantic_agent._run_async_impl

async def debug_semantic_run(ctx):
    print("\n=== SemanticAgent DEBUG ===")
    print("Input context passed from RootAgent:")
    print(ctx)

    # This preserves async generator behavior expected by ADK
    async for event in original_semantic_run(ctx):
        # --- Inspect raw event ---
        print("Event:", event)

        # --- If event contains LLM response, inspect it ---
        llm_response = getattr(event, "llm_response", None)
        if llm_response:
            for i, candidate in enumerate(llm_response.candidates):
                for j, part in enumerate(candidate.content.parts):
                    part_type = getattr(part, "type", "<unknown>")
                    part_text = getattr(part, "text", "")
                    part_metadata = getattr(part, "metadata", None)
                    print(f"[Candidate {i} Part {j}] Type: {part_type}")
                    print(f"Text: {part_text}")
                    print(f"Metadata: {part_metadata}")

            # Concatenate output_text parts
            llm_text = ""
            for candidate in llm_response.candidates:
                for part in candidate.content.parts:
                    if getattr(part, "type", "") == "output_text" and getattr(part, "text", ""):
                        llm_text += part.text

            print("\n=== Concatenated output_text ===")
            print(llm_text)

            # Try parsing as JSON
            try:
                parsed_json = json.loads(llm_text)
                print("\n✅ Parsed JSON from SemanticAgent output:")
                print(json.dumps(parsed_json, indent=2))
            except json.JSONDecodeError:
                print("\n⚠️ Could not parse as JSON. Raw text returned instead.")

        # Yield the event to preserve async generator interface
        yield event

# Patch the agent
semantic_agent._run_async_impl = debug_semantic_run


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

