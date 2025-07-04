from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import logging
import asyncio
import uvicorn
from dotenv import load_dotenv
import os
import re

# Load environment variables from .env file
load_dotenv()

# Import custom helper functions for Databricks Genie integration
from helper import fetch_answer
from table_extraction import get_tables, get_table_columns

# Configure logging for debugging and monitoring
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI application with Jinja2 templates
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Global variables for managing conversation state and configuration
CURRENT_CONVERSATION_ID = None  # Tracks ongoing conversation with Genie
DYNAMIC_GENIE_ROOM_ID = None    # User-provided Genie Room ID
SCHEMA_INFO = ""                # Cached schema information for the dataset

# Load Databricks configuration from environment variables
access_token = os.getenv("ACCESS_TOKEN")
workspace_url = os.getenv("WORKSPACE_URL")

# Log configuration status (for debugging)
print("Access Token:", "***" if access_token else "Not Set")
print("Workspace URL:", workspace_url if workspace_url else "Not Set")
print("Genie Room ID will be set dynamically by user")


# Initialize OpenAI client for Databricks serving endpoints
from openai import OpenAI

# Configure OpenAI client to use Databricks serving endpoints
client = OpenAI(
    api_key=access_token,
    base_url=f"{workspace_url}serving-endpoints"
)

def determine_required_columns(query, table_schema):
    """
    Analyze the user query and determine which columns from the schema are required
    
    Args:
        query (str): The user's query
        table_schema (dict): The schema of available tables with their columns
        
    Returns:
        list: A list of required column names
    """
    global SCHEMA_INFO
    
    # Build schema_info from the passed table_schema parameter
    schema_info = ""
    
    if table_schema and isinstance(table_schema, dict) and len(table_schema) > 0:
        # Use the actual table schema passed to the function
        logger.info(f"Using passed table_schema with {len(table_schema)} tables")
        for table_name, columns in table_schema.items():
            schema_info += f"Table: {table_name}\n"
            if columns and len(columns) > 0:
                schema_info += f"Columns: {', '.join(columns)}\n"
            else:
                schema_info += "Columns: [No column information available]\n"
            schema_info += "\n"
        
        # Also extract all unique column names for easier processing
        all_columns = set()
        for columns in table_schema.values():
            if columns:
                all_columns.update(columns)
        
        if all_columns:
            schema_info += f"\nAll Available Columns: {', '.join(sorted(all_columns))}\n"
            
    elif SCHEMA_INFO:
        # Use the global SCHEMA_INFO if table_schema is empty but SCHEMA_INFO is available
        logger.info("Using global SCHEMA_INFO as fallback")
        schema_info = f"Available Columns: {SCHEMA_INFO}"
    else:
        # Use hardcoded fallback as last resort
        logger.warning("Using hardcoded fallback schema")
        schema_info = """Available Columns: order_id, order_datetime, abnormal_flag, first_name, diagnosis_type, message_type, value, address, guarantor_phone, diagnosis_code, discharge_datetime, reference_range, ordering_provider, test_name, recorded_datetime, diagnosis_description, source_file, guarantor_address, sending_fac, phone, guarantor_name, admit_datetime, unit, receiving_app, attending_doctor, observation_id, assigned_location, last_name, patient_class, message_datetime, hl7_version, sending_app, dob, gender, patient_id, event_type"""
    
    chat_completion = client.chat.completions.create(
      messages=[
      {
        "role": "system",
        "content": f"""
You are a Business Analytics and AI expert in healthcare data analysis. Your task is to review the available database schema and the user's query, and identify only the column names required to answer the query.

Database Schema Information:
{schema_info}

Instructions:
- Analyze the user's query and identify which columns from the available schema are needed to answer it
- Return a JSON array containing ONLY the column names needed to answer the query, formatted as:
  ["column1", "column2", "column3", ...]
- Only include columns that are directly necessary for answering the query
- For patient-related queries, include relevant patient identifiers (patient_id) and demographic columns (first_name, last_name, dob, gender)
- For encounter/admission queries, include encounter identifiers and related columns (admit_datetime, discharge_datetime, attending_doctor, assigned_location)
- For diagnosis queries, include diagnosis-related columns (diagnosis_code, diagnosis_description, diagnosis_type)
- For test/lab queries, include test-related columns (test_name, value, reference_range, abnormal_flag)
- If the query involves aggregations, grouping, or calculations, include the columns required for those operations
- If the query asks for dataset structure or schema information, include representative columns from different categories
- Do NOT specify any table names in your response
- Do NOT add any explanation, description, or extra text—output ONLY the JSON array of column names
- If no specific columns are needed (e.g., for general information queries), return an empty array []

When you receive a query, analyze it against the provided schema and return only the necessary column names in the exact JSON array format.
"""

      },
      {
        "role": "user",
        "content": query
      }
      ],
      model="my-gpt-endpoint",
      max_tokens=2048
    )
    
    response = chat_completion.choices[0].message.content
    logger.info(f"Required columns for query '{query}': {response}")
    
    # Try to parse the response as JSON
    try:
        import json
        # Clean the response to handle potential text before or after the JSON
        json_str = response.strip()
        # Find JSON content between square brackets
        start = json_str.find('[')
        end = json_str.rfind(']') + 1
        if start >= 0 and end > start:
            json_str = json_str[start:end]
            
        required_columns = json.loads(json_str)
        return required_columns
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse required columns response as JSON: {response}")
        logger.error(f"JSON error: {str(e)}")
        # Return empty list if parsing fails
        return []

def generate_dynamic_questions(schema_info=None):
    """
    Generate dynamic questions based on the provided schema information
    
    Args:
        schema_info (str): Schema information containing column names
        
    Returns:
        list: A list of generated questions based on the schema
    """
    global SCHEMA_INFO
    
    # Use provided schema_info or fall back to global SCHEMA_INFO
    if not schema_info and SCHEMA_INFO:
        schema_info = SCHEMA_INFO
    elif not schema_info:
        schema_info = "No schema information available"
    
    print("Schema information passed to generate_dynamic_questions:")
    print(schema_info)
    
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": f"""
You are a Data Analyst. Your job is to create simple, plain English questions that can be answered using SQL queries. I will provide you with a sample dataset (column names only), and you will generate 5 simple SQL-friendly questions based on the available columns.

Database Schema Information:
{schema_info}

Instructions:
- Make the questions easy to understand and suitable for beginner to intermediate SQL users.
- Generate 5 simple questions in plain English based on the above columns.
- Return the questions as a list for example: ["Question 1", "Question 2", "Question 3", "Question 4", "Question 5"]
- Focus on common business analytics queries like counts, aggregations, filtering, and basic insights.
- Make sure the questions are relevant to the available columns.

Give me the list of the questions in the list format only. Dont give any other part than generated questions. 
"""
            },
            {
                "role": "user",
                "content": "Generate 5 simple questions based on the provided schema information."
            }
        ],
        model="my-gpt-endpoint",
        max_tokens=2048,
        temperature = 0,
    )
    
    response = chat_completion.choices[0].message.content
    print(f"Generated questions response: {response}")
    
    # Try to parse the response as a Python list
    try:
        import ast
        # Try to evaluate as Python literal
        if response.strip().startswith('[') and response.strip().endswith(']'):
            questions = ast.literal_eval(response.strip())
            if isinstance(questions, list):
                return questions
        
        # If not a proper list format, try to extract questions manually
        lines = response.strip().split('\n')
        questions = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('//'):
                # Remove any numbering or bullet points
                import re
                clean_line = re.sub(r'^\d+\.?\s*', '', line)
                clean_line = re.sub(r'^[-*]\s*', '', clean_line)
                clean_line = clean_line.strip('"\'')
                if clean_line:
                    questions.append(clean_line)
        
        if questions:
            return questions[:5]  # Return max 5 questions
            
    except Exception as e:
        logger.error(f"Failed to parse generated questions: {str(e)}")
    
    # Fallback to predefined questions if parsing fails
    logger.info("Using predefined sample questions as fallback")
    return [
        "Show me the first 10 rows of the dataset",
        "How many total records are in the dataset?",
        "What are the unique values in the main categories?",
        "Show me summary statistics for the numerical columns",
        "What is the data distribution by key fields?"
    ]




async def direct_genie_answer(question, required_columns=None):
    """
    Process user question directly with Databricks Genie without external search
    
    Args:
        question (str): The user's question
        required_columns (list, optional): List of required column names
    """
    global CURRENT_CONVERSATION_ID, DYNAMIC_GENIE_ROOM_ID
    
    try:
        # Check if Genie Room ID is set
        if not DYNAMIC_GENIE_ROOM_ID:
            raise Exception("Genie Room ID not set. Please configure it first.")
            
        logger.info("🚀 Sending direct request to Genie...")
        logger.info(f"Using existing conversation ID: {CURRENT_CONVERSATION_ID}")
        
        # Append "using the data" to the question as before
        question = question + " using the data."
        
        # If required columns are provided, append them to the question
        if required_columns and isinstance(required_columns, list) and len(required_columns) > 0:
            columns_str = ", ".join(required_columns)
            question = f"{question} Please focus on these columns : {columns_str}."
            logger.info(f"Enhanced question with column info: {question}")
        
        # Use the existing conversation ID if available with dynamic Genie ID
        response = await fetch_answer(workspace_url, DYNAMIC_GENIE_ROOM_ID, access_token, 
                                     question, CURRENT_CONVERSATION_ID)
        
        # Save the conversation ID for future use
        if isinstance(response, dict) and "conversation_id" in response:
            CURRENT_CONVERSATION_ID = response["conversation_id"]
            logger.info(f"✅ Updated conversation ID: {CURRENT_CONVERSATION_ID}")
            
        return response
    except Exception as e:
        logger.exception("Error in direct_genie_answer")
        raise e


@app.post("/set-genie-id")
async def set_genie_id(request: Request):
    """Set the dynamic Genie Room ID"""
    global DYNAMIC_GENIE_ROOM_ID, CURRENT_CONVERSATION_ID
    
    try:
        body = await request.json()
        genie_room_id = body.get("genie_room_id", "").strip()
        
        if not genie_room_id:
            return JSONResponse({"success": False, "error": "Genie Room ID is required"}, status_code=400)
        
        # Set the dynamic Genie Room ID
        DYNAMIC_GENIE_ROOM_ID = genie_room_id
        
        # Reset conversation when new Genie ID is set
        CURRENT_CONVERSATION_ID = None
        
        logger.info(f"✅ Genie Room ID set to: {DYNAMIC_GENIE_ROOM_ID}")
        
        return JSONResponse({
            "success": True, 
            "message": f"Genie Room ID set successfully: {genie_room_id}"
        })
        
    except Exception as e:
        logger.exception("Error setting Genie Room ID")
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@app.get("/")
async def home(request: Request):
    """Render the home page with chat interface"""
    global CURRENT_CONVERSATION_ID
    CURRENT_CONVERSATION_ID = None  # Reset the conversation when the home page is loaded
    logger.info("🔄 Conversation ID reset on page load")
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/query")
async def query(request: Request):
    """Process user query and return response"""
    try:
        # Check if Genie Room ID is set
        if not DYNAMIC_GENIE_ROOM_ID:
            return JSONResponse({
                "error": "Genie Room ID not configured. Please set it first."
            }, status_code=400)
            
        body = await request.json()
        user_query = body.get("query", "").strip()
        
        # Get catalog and schema if provided
        catalog_name = body.get("catalog_name")
        schema_name = body.get("schema_name")
        
        # Add option to reset conversation if needed
        reset_conversation = body.get("reset_conversation", False)
        
        if reset_conversation:
            global CURRENT_CONVERSATION_ID
            CURRENT_CONVERSATION_ID = None
            logger.info("🔄 Conversation reset requested - starting new conversation")
            return JSONResponse({
                "response_type": "text",
                "message": "Conversation has been reset. Starting a new conversation."
            })
        
        if not user_query:
            return JSONResponse({"error": "Query parameter is required"}, status_code=400)
        
        # If catalog and schema are provided, include them in the query context
        if catalog_name and schema_name:
            context_message = f"Using catalog '{catalog_name}' and schema '{schema_name}': "
            user_query_with_context = f"{context_message}{user_query}"
            logger.info(f"Query with catalog/schema context: {user_query_with_context}")
        else:
            user_query_with_context = user_query
        
        # Get table schema if catalog and schema are provided
        table_schema = {}
        if catalog_name and schema_name:
            try:
                tables_result = get_tables(catalog_name, schema_name)
                if tables_result["success"]:
                    # Format table schema for the LLM
                    table_schema = {}
                    for table in tables_result["tables"]:
                        table_name = table["name"]
                        # Fetch column information for this table
                        try:
                            # Get columns using the new function
                            columns_result = get_table_columns(catalog_name, schema_name, table_name)
                            
                            if columns_result["success"]:
                                columns = columns_result["columns"]
                                table_schema[table_name] = columns
                                logger.info(f"Retrieved columns for {table_name}: {columns}")
                            else:
                                # Fallback to DESCRIBE TABLE if API fails
                                logger.warning(f"API call failed for {table_name}: {columns_result.get('error')}")
                                
                                # Execute a simple DESCRIBE TABLE query through Genie to get columns
                                describe_query = f"DESCRIBE TABLE {catalog_name}.{schema_name}.{table_name}"
                                describe_response = await fetch_answer(workspace_url, DYNAMIC_GENIE_ROOM_ID, access_token, 
                                                                    describe_query, None)
                                
                                # Parse the response to extract column names
                                columns = []
                                if isinstance(describe_response, dict) and "statement_response" in describe_response:
                                    stmt = describe_response["statement_response"]
                                    if stmt and "result" in stmt and "data_array" in stmt["result"]:
                                        # Assuming first column in each row is the column name
                                        columns = [row[0] for row in stmt["result"]["data_array"] if row]
                                
                                table_schema[table_name] = columns
                                logger.info(f"Retrieved columns for {table_name} using fallback: {columns}")
                        except Exception as col_err:
                            logger.warning(f"Failed to get columns for table {table_name}: {str(col_err)}")
                            table_schema[table_name] = []
            except Exception as e:
                logger.warning(f"Failed to get table schema: {str(e)}")
        
        # Determine required columns
        logger.info(f"Table schema being passed: {table_schema}")
        required_columns = determine_required_columns(user_query_with_context, table_schema)
        logger.info(f"Required columns determined: {required_columns}")
        
        # Send query directly to Genie
        response = await direct_genie_answer(user_query_with_context, required_columns)
        
        logger.info(f"✅ Raw response received: {response}")
        
        # Include conversation ID in the response for debugging/frontend purposes
        if isinstance(response, dict) and "conversation_id" in response:
            conversation_id = response["conversation_id"]
        else:
            conversation_id = CURRENT_CONVERSATION_ID
        
        # Try to parse as structured table
        try:
            stmt = response.get("statement_response") if isinstance(response, dict) else None
            if stmt:
                columns_info = stmt.get("manifest", {}).get("schema", {}).get("columns")
                data_array = stmt.get("result", {}).get("data_array")

                if columns_info and data_array:
                    columns = [col["name"] for col in columns_info]
                    df = pd.DataFrame(data_array, columns=columns)
                    return JSONResponse({
                        "response_type": "table",
                        "columns": df.columns.tolist(),
                        "data": df.to_dict(orient="records"),
                        "original_query": user_query,
                        "conversation_id": conversation_id,
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "required_columns": required_columns
                    })

            # If response has an 'answer' field
            if isinstance(response, dict) and "answer" in response:
                return JSONResponse({
                    "response_type": "text",
                    "message": response["answer"],
                    "original_query": user_query,
                    "conversation_id": conversation_id,
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "required_columns": required_columns
                })

            # Fallback: return as-is (for general info or raw LLM output)
            return JSONResponse({
                "response_type": "text",
                "message": str(response),
                "original_query": user_query,
                "conversation_id": conversation_id,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "required_columns": required_columns
            })
        
        except Exception as parse_err:
            logger.exception("⚠️ Failed to parse response structure")
            return JSONResponse({
                "response_type": "text",
                "message": "⚠️ Unexpected response format.",
                "raw_response": str(response),
                "original_query": user_query,
                "conversation_id": conversation_id,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "required_columns": required_columns
            })

    except Exception as e:
        logger.exception("🔥 Error processing query")
        return JSONResponse({
            "response_type": "text", 
            "message": f"❌ Error: {str(e)}"
        }, status_code=500)


@app.get("/reset-conversation")
async def reset_conversation():
    """Endpoint to explicitly reset the conversation"""
    global CURRENT_CONVERSATION_ID
    CURRENT_CONVERSATION_ID = None
    logger.info("🔄 Conversation has been reset")
    return JSONResponse({"status": "success", "message": "Conversation reset successful"})


@app.get("/conversation-status")
async def conversation_status():
    """Return the current conversation ID status"""
    global CURRENT_CONVERSATION_ID
    return JSONResponse({
        "has_active_conversation": CURRENT_CONVERSATION_ID is not None,
        "conversation_id": CURRENT_CONVERSATION_ID
    })


@app.get("/sample-questions")
async def sample_questions(section: str):
    """Return sample questions for the user interface based on section"""
    global SCHEMA_INFO
    
    # Generate dynamic questions based on schema
    questions = generate_dynamic_questions(SCHEMA_INFO)
    return JSONResponse({"questions": questions})

@app.get("/catalogs")
async def get_catalogs():
    """Return a list of available catalogs"""
    # In a real application, you would query Databricks for available catalogs
    # For demonstration, returning hardcoded values
    catalogs = [
        {"name": "users_trial", "display_name": "Users Trial"},
        {"name": "main", "display_name": "Main"},
        {"name": "hive_metastore", "display_name": "Hive Metastore"}
    ]
    return JSONResponse({"catalogs": catalogs})

@app.get("/schemas/{catalog_name}")
async def get_schemas(catalog_name: str):
    """Return schemas for a given catalog"""
    # In a real application, you would query Databricks for schemas in the catalog
    # For demonstration, returning hardcoded values
    schemas = []
    if catalog_name == "users_trial":
        schemas = [
            {"name": "nitin_aggarwal", "display_name": "Nitin Aggarwal"},
            {"name": "default", "display_name": "Default"}
        ]
    elif catalog_name == "main":
        schemas = [
            {"name": "default", "display_name": "Default"},
            {"name": "samples", "display_name": "Samples"}
        ]
    elif catalog_name == "hive_metastore":
        schemas = [
            {"name": "default", "display_name": "Default"}
        ]
    return JSONResponse({"schemas": schemas})

@app.get("/tables/{catalog_name}/{schema_name}")
async def get_catalog_tables(catalog_name: str, schema_name: str):
    """Return tables for a given catalog and schema"""
    try:
        result = get_tables(catalog_name, schema_name)
        if result["success"]:
            # Format the tables for frontend display
            formatted_tables = []
            for table in result["tables"]:
                formatted_tables.append({
                    "name": table.get("name"),
                    "full_name": table.get("full_name"),
                    "table_type": table.get("table_type")
                })
            return JSONResponse({"tables": formatted_tables})
        else:
            return JSONResponse({"error": result["error"]}, status_code=400)
    except Exception as e:
        logger.exception("Error fetching tables")
        return JSONResponse({"error": str(e)}, status_code=500)



@app.post("/fetch-schema")
async def fetch_schema(request: Request):
    """Fetch schema information by getting first 5 rows of data"""
    global DYNAMIC_GENIE_ROOM_ID, SCHEMA_INFO
    
    try:
        body = await request.json()
        genie_room_id = body.get("genie_room_id", "").strip()
        
        if not genie_room_id:
            return JSONResponse({"success": False, "error": "Genie Room ID is required"}, status_code=400)
        
        # Set the dynamic Genie Room ID
        DYNAMIC_GENIE_ROOM_ID = genie_room_id
        logger.info(f"✅ Using Genie Room ID: {DYNAMIC_GENIE_ROOM_ID}")
        
        # Question to get first 5 rows
        # question = "give me the first 5 rows of the given data"
        question = "give me all the columns from all the tables in the dataset using full joins"
        
        logger.info("🚀 Fetching first 5 rows to extract schema...")
        
        # Fetch answer using the provided genie_room_id
        response = await fetch_answer(workspace_url, genie_room_id, access_token, question, None)
        
        # logger.info(f"✅ Response received: {type(response)}")
        print("================schema infor", response)
        
        # Extract column names from the response
        columns = []
        try:
            if isinstance(response, dict) and "statement_response" in response:
                stmt = response["statement_response"]
                if stmt:
                    # Get columns from manifest schema
                    columns_info = stmt.get("manifest", {}).get("schema", {}).get("columns")
                    if columns_info:
                        columns = [col["name"] for col in columns_info]
                        logger.info(f"✅ Extracted columns from manifest: {columns}")
                    
                    # If no columns from manifest, try to get from data_array header
                    elif "result" in stmt and "data_array" in stmt["result"]:
                        data_array = stmt["result"]["data_array"]
                        if data_array and len(data_array) > 0:
                            # Assuming first row might be headers or we can infer column count
                            first_row = data_array[0]
                            if first_row:
                                # Generate column names if we can't get them directly
                                columns = [f"column_{i+1}" for i in range(len(first_row))]
                                logger.info(f"✅ Inferred columns from data structure: {columns}")
                    
                    # Additional fallback: check if there's a schema field elsewhere
                    elif "manifest" in stmt:
                        manifest = stmt["manifest"]
                        print("================manifest structure", manifest)
                        # Try to find schema information in different places
                        if "schema" in manifest:
                            schema = manifest["schema"]
                            print("================schema structure", schema)
        
        except Exception as e:
            logger.error(f"Error extracting columns: {str(e)}")
            print("================Error extracting columns:", str(e))
        
        if columns:
            # Update the global SCHEMA_INFO variable
            SCHEMA_INFO = "\t".join(columns)
            logger.info(f"✅ Updated global SCHEMA_INFO: {SCHEMA_INFO}")
            
            return JSONResponse({
                "success": True,
                "message": "Schema information fetched successfully",
                "columns": columns,
                "schema_info": SCHEMA_INFO,
                "genie_room_id": genie_room_id
            })
        else:
            return JSONResponse({
                "success": False, 
                "error": "Could not extract column information from the response",
                "raw_response": str(response)
            }, status_code=400)
            
    except Exception as e:
        logger.exception("Error fetching schema information")
        return JSONResponse({
            "success": False, 
            "error": f"Failed to fetch schema: {str(e)}"
        }, status_code=500)

if __name__ == "__main__":
    # uvicorn.run(app, host="0.0.0.0", port=8000)   #for databricks
    uvicorn.run(app, port=8000)
    # uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
    