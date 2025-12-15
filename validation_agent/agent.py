import os
import google.auth
from google.adk.agents import Agent
from google.adk.tools.bigquery import BigQueryToolset

bigquery_toolset = BigQueryToolset(  # Note: Ensure this argument name matches your library version (sometimes just 'tool_config')
    tool_filter=[
        'list_dataset_ids',
        'get_dataset_info',
        'list_table_ids',
        'get_table_info',
        'execute_sql',
    ]
)

root_agent = Agent(
   model="gemini-2.0-flash",
   name="bigquery_agent",
   description=(
       "Agent that answers questions about BigQuery data by executing SQL queries"
   ),
   instruction=""" You are a data analysis agent with access to several BigQuery tools. Make use of those tools to answer the user's questions. """,
   tools=[bigquery_toolset],
)