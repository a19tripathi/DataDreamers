from google.adk import Agent
from .prompts import DATA_PROFILING_SYSTEM_PROMPT
from .tools import add
import os
from dotenv import load_dotenv
from toolbox_core import ToolboxSyncClient

load_dotenv()

PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
DATASET_ID = "test_dataset"

toolbox = ToolboxSyncClient("http://127.0.0.1:5000")

# Load all the tools
tools = toolbox.load_toolset('profiling_toolset')


root_agent = Agent(
    model='gemini-2.5-flash',
    name='root_agent',
    description='A data profiler agent that will summarise table information using BigQuery.',
    instruction=DATA_PROFILING_SYSTEM_PROMPT,
    tools=tools,
)
