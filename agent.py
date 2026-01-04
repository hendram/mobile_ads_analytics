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
import mysql.connector

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
   - If user ask something like "please calculate total cost of fuel if per 1000 meters will cost 2usd for car 2 and return all waypoint too with table distance, from, to, cost 
     in form of html table like <table><td><tr>...</td></tr></table> for number of tr and td adjusted based on result and 
     without markdown ```html or ```json, anything markdown and don't skip any single leg, 
     comparing with car 1 if cost of fuel for car 1 per 2000 meters will cost 3 usd and make table too with 
     table distance, from, to, cost and calculate different total cost between car 1 and car 2, which one more expensive for total route 
     then you can run tools combined_sequential_agent and run it combined_sequential_agent again for another car 
     and intelligently divide user asked for combined_sequential_agent and second call combined_sequential_agent
     based on carId  and    
    make it like json { id: carId, userquery: user asked match with carId}  and input it to combined_sequential_agent,
           { id: carId, userquery: user asked match with carId } input it again to combined_sequential_agent 
    which each id different car id and different user asked

4. Scenario 4: Counting how many trip specific car has drove in
     - If user ask like "please give me how many trip this car has drive in ", then you can run tool 
       calculated_trip_carseq by giving input in format:
       carId without spaces (e.g., car1), make it like json { id: carId }

5. Scenario 5: Counting how much elapse time has been since first car drove till last position
     - If user ask like "Please get me elapse time for car1 latest trip", then you can run tools calculated_elapse_timeseq
       to get data by giving input in format:
       carId without spaces (e.g., car1), make it like json { id: carId }

6.Scenario 6: If user for example give this query: "calculate total fuel cost for car 1 latest trip along with all legs  if fuel price for 
 car 1 is 3 USD for every 1500 m and report as graph points location consist of x and y"  then always send to 
 tool combined_sequential_agent with input in format: carId without spaces (e.g., car1) and user ask which 
 added more clearer instruction like in format like [{x: 3434, y: 299}, { } ...]  but not cumulative, except user asked
  without other words attached to for every leg 
 and always return this [{x: 3434, y: 299}, { } ...] in markdown suitable for plotly so make
        it like json { id: carId, userquery: user asked plus your additional instruction} 


""" 

# ------------------------- 
# BaseAgents (specialized agents)
# -------------------------
DIRECTION_API = os.getenv("DIRECTION_API")

executor = ThreadPoolExecutor(max_workers=2)

import requests, os, json

TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASS = os.getenv("TIDB_PASS")
TIDB_DB = os.getenv("TIDB_DB_NAME", "test")
TIDB_PORT = int(os.getenv("TIDB_PORT", 4000))
TIDB_SSL_CA = os.getenv("TIDB_SSL_CA", "./ca.pem")

def get_tidb_connection():
    return mysql.connector.connect(
        host=TIDB_HOST,
        user=TIDB_USER,
        password=TIDB_PASS,
        database=TIDB_DB,
        port=TIDB_PORT,
        ssl_ca=TIDB_SSL_CA
    )

# -------------------------
# BaseAgents (specialized agents)
# -------------------------
class TiDBDistanceAnalyticsAgent(BaseAgent):
    name: str = "TiDBDistanceAnalyticsAgent"
    description: str = (
        "For the given carId, find the latest trip collection "
        "and return all legs with from, to, distance."
    )
   
    async def _run_async_impl(self, ctx):
        raw = getattr(ctx.user_content.parts[0], "text", "").strip()
        if not raw:
            yield Event(
                author=self.name,
                content=types.Content(parts=[types.Part(text=json.dumps({"error": "JSON input required: {carId}"}))])
            )
            return
        
        payload = json.loads(raw)
        carId = payload.get("id")
        user_query = payload.get("userquery")
        ctx.session.state["temp:user_query"] = user_query
        conn = None
        cursor = None

        try:
            conn = get_tidb_connection()
            cursor = conn.cursor(dictionary=True)

            # Latest trip collection
            cursor.execute("""
                SELECT DISTINCT trip_collection 
                FROM car_trip_legs 
                WHERE car_id=%s 
                ORDER BY trip_collection DESC LIMIT 1
            """, (carId,))
            row = cursor.fetchone()
            if not row:
                ctx.session.state["temp:legs"] = []
                return

            latest_trip = row["trip_collection"]

            cursor.execute("""
                SELECT leg_index, origin, destination, distance_m 
                FROM car_trip_legs 
                WHERE car_id=%s AND trip_collection=%s 
                ORDER BY leg_index ASC
            """, (carId, latest_trip))

            legs = cursor.fetchall()
            results = [{"carId": carId, "leg_index": leg["leg_index"], "from": leg["origin"], "to": leg["destination"], "distance": leg["distance_m"]} for leg in legs]

            ctx.session.state["temp:legs"] = results
            logger.info("SESSION STATE DUMP: %s", json.dumps(ctx.session.state, default=str))

        except Exception as e:
            ctx.session.state["temp:legs"] = {"error": str(e)}
        finally:
            if cursor is not None:
               cursor.close()
            if conn is not None:
               conn.close()


Predictivecost_agent = LlmAgent(
    name="PredictivecostAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=(
      """    Your job is to get user query from {temp:user_query} and calculate all requested with this data {temp:legs} ,
        don't drop any single leg and calculate step by step from leg_index 1 till last number leg_index finished 
        using formula (cost/km * total legs distance),
        which you need to add all distance step by step first from leg_index 1 till last number leg_index
         to get total legs distance, 
        and adapt it or convert it based on user input,
        like if cost/m or cost/feet, then convert to adjust it before using formula (cost/km * total legs distance).
        If {temp:legs} is empty or missing, say so clearly.
        Output only the data in JSON or text form as asked in  user query, no need to give more, if for example total distance
        not asked, then no need to output it.
      """ 
    )
)

class TiDBCalculatedTripCarAgent(BaseAgent):
    name: str = "TiDBCalculatedTripCarAgent"
    description: str = "Count how many trips a specific car has driven."

    async def _run_async_impl(self, ctx):
        raw = getattr(ctx.user_content.parts[0], "text", "").strip()
        conn = None
        cursor = None
        if not raw:
            ctx.session.state["temp:num_drives"] = {"error": "JSON input required: {carId}"}
            yield Event(
               author=self.name,
               content=types.Content(parts=[types.Part(text=json.dumps({"error": "JSON input required: {carId}"}))])
            )
            return
        payload = json.loads(raw)
        carId = payload.get("id")
        if not carId:
            ctx.session.state["temp:num_drives"] = {"error": "Missing 'id' in input JSON."}
            return

        try:
            conn = get_tidb_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(DISTINCT trip_doc) AS num_drives
                FROM car_latest_positions
                WHERE car_id=%s
            """, (carId,))
            row = cursor.fetchone()
            ctx.session.state["temp:num_drives"] = {"carId": carId, "num_drives": row[0] if row else 0}
            logger.info("SESSION STATE DUMP: %s", json.dumps(ctx.session.state, default=str))

        except Exception as e:
            ctx.session.state["temp:num_drives"] = {"error": str(e)}
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

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


class TiDBElapsedTimeAgent(BaseAgent):
    name: str = "TiDBElapsedTimeAgent"
    description: str = "Calculate elapsed time for latest trip of a car."
   
    async def _run_async_impl(self, ctx):
        raw = getattr(ctx.user_content.parts[0], "text", "").strip()
        conn = None
        cursor = None   
        if not raw:
            ctx.session.state["temp:elapsed_time"] = {"error": "JSON input required: {carId}"}
            return

        payload = json.loads(raw)
        carId = payload.get("id")
        if not carId:
            ctx.session.state["temp:elapsed_time"] = {"error": "Missing 'id' in input JSON."}
            return

        try:
            conn = get_tidb_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
                FROM car_latest_positions
                WHERE car_id=%s
            """, (carId,))
            row = cursor.fetchone()
            if not row or not row["first_ts"] or not row["last_ts"]:
                ctx.session.state["temp:elapsed_time"] = {"error": "No positions found"}
                return

            elapsed = row["last_ts"] - row["first_ts"]
            hours, rem = divmod(elapsed.total_seconds(), 3600)
            minutes, seconds = divmod(rem, 60)
            ctx.session.state["temp:elapsed_time"] = {"carId": carId, "elapsed_time": {"hours": int(hours), "minutes": int(minutes), "seconds": int(seconds)}}
            logger.info("SESSION STATE DUMP: %s", json.dumps(ctx.session.state, default=str))

        except Exception as e:
            ctx.session.state["temp:elapsed_time"] = {"error": str(e)}
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

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
tidbdistanceanalytics_tool = agent_tool.AgentTool(agent=TiDBDistanceAnalyticsAgent())
tidbcalculatedtripcar_tool = agent_tool.AgentTool(agent=TiDBCalculatedTripCarAgent())
calculatedtripcarreport_tool = agent_tool.AgentTool(agent=CalculatedTripCarReport_agent)
tidbelapsedtime_tool = agent_tool.AgentTool(agent=TiDBElapsedTimeAgent())
calculatedelapsetime_tool = agent_tool.AgentTool(agent=CalculatedElapseTimeReport_agent)


combined_sequential_agent = SequentialAgent(
    name="combinedSequentialAgent",
    sub_agents=[TiDBDistanceAnalyticsAgent(), Predictivecost_agent],
    description="Run TiDB fetch first, then process with TestFetch agent using the same session temp. Run it once then finish"
)

calculatedtripcarseq_agent = SequentialAgent(
    name="calculatedtripcarSequentialAgent",
    sub_agents=[TiDBCalculatedTripCarAgent(), CalculatedTripCarReport_agent],
    description="Run TiDBCalculatedTripCar first, then process with CalculatedTripCarReport agent using the same session temp."
)

calculatedtripelapsetimeseq_agent = SequentialAgent(
    name="calculatedtripelapsetimeSequentialAgent",
    sub_agents=[TiDBElapsedTimeAgent(), CalculatedElapseTimeReport_agent],
    description="Run TiDBElapseTime first, then process with CalculatedElapseTimeReport agent using the same session temp."
)


combined_sequential_agent_tool = agent_tool.AgentTool(agent=combined_sequential_agent)
calculated_trip_carseq_tool = agent_tool.AgentTool(agent=calculatedtripcarseq_agent)
calculated_elapse_timeseq_tool = agent_tool.AgentTool(agent=calculatedtripelapsetimeseq_agent)

# Semantic LLM agent uses the sequential agent tool
semantic_agent = LlmAgent(
    name="SemanticAgent",
    model=os.getenv("ADK_MODEL", "gemini-2.5-flash"),
    instruction=DIRECTIONS_AGENT_INSTRUCTION,
    tools=[combined_sequential_agent_tool, calculated_trip_carseq_tool, calculated_elapse_timeseq_tool]   
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

