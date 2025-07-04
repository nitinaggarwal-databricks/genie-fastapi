
import os
import aiohttp
import json
import asyncio
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



async def fetch_answer(workspace_url, genie_room_id, access_token, input_text, conversation_id=None):
    """
    Asynchronously fetch answer from Databricks Genie
    Maintains the same conversation if conversation_id is provided
    
    Parameters:
    -----------
    workspace_url : str
        URL of the Databricks workspace
    genie_room_id : str
        ID of the Genie room/space
    access_token : str
        Authorization token for API access
    input_text : str
        Text input to send to Genie
    conversation_id : str, optional
        ID of an existing conversation to continue (default: None)
        
    Returns:
    --------
    dict
        Response from the API with conversation_id included
    """
    # Fix 1: Ensure workspace_url ends with a slash
    if not workspace_url.endswith('/'):
        workspace_url = workspace_url + '/'
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Add retry counter for failed responses
    max_query_retries = 3
    query_retry_count = 0
    
    # Create SSL context that doesn't verify certificates
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            # If conversation_id is provided, continue existing conversation
            if conversation_id:
                logger.info(f"Continuing conversation with ID: {conversation_id}")
                # Send message to existing conversation
                continue_url = f"{workspace_url}api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages"
                payload_continue = {"content": input_text}
                
                logger.info(f"Making request to: {continue_url}")
                logger.info(f"Payload: {payload_continue}")
                
                async with session.post(continue_url, json=payload_continue, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Error continuing conversation: Status {response.status}, Response: {error_text}")
                        return {"error": f"API error: {response.status} - {error_text}", "conversation_id": conversation_id}
                    
                    response_text = await response.text()
                    try:
                        response_json = json.loads(response_text)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON response: {response_text}")
                        return {"error": f"Invalid JSON response: {str(e)}", "conversation_id": conversation_id}
                
                # Get the message_id from the response
                if "message_id" not in response_json:
                    logger.error(f"Unexpected response format - missing message_id: {response_json}")
                    return {"error": "Unexpected response format from API", "conversation_id": conversation_id}
                    
                message_id = response_json['message_id']
                
            else:
                # Start a new conversation if no conversation_id is provided
                start_conv_url = f"{workspace_url}api/2.0/genie/spaces/{genie_room_id}/start-conversation"
                payload_start = {"content": input_text}
                
                logger.info(f"Making request to: {start_conv_url}")
                logger.info(f"Payload: {payload_start}")
                
                # Start the conversation
                logger.info("Starting new conversation with Genie")
                async with session.post(start_conv_url, json=payload_start, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Error starting conversation: Status {response.status}, Response: {error_text}")
                        return {"error": f"API error: {response.status} - {error_text}"}
                    
                    response_text = await response.text()
                    try:
                        response_json = json.loads(response_text)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON response: {response_text}")
                        return {"error": f"Invalid JSON response: {str(e)}"}
    
                # Validate expected response format
                if "message_id" not in response_json:
                    logger.error(f"Unexpected response format - missing message_id: {response_json}")
                    return {"error": "Unexpected response format from API"}
                    
                message_id = response_json['message_id']
                
                # Fix 2: Handle different response structures
                if "message" in response_json and "conversation_id" in response_json["message"]:
                    conversation_id = response_json["message"]['conversation_id']
                elif "conversation_id" in response_json:
                    conversation_id = response_json["conversation_id"]
                else:
                    logger.error(f"Could not find conversation_id in response: {response_json}")
                    return {"error": "Missing conversation_id in API response"}
    
            logger.info(f"Conversation ID: {conversation_id}, Message ID: {message_id}")

            # Continue trying until max retries reached
            while query_retry_count < max_query_retries:
                if query_retry_count > 0:
                    logger.info(f"Retry attempt {query_retry_count} of {max_query_retries} for conversation ID: {conversation_id}")
                
                # Polling for answer (same for both new and continued conversations)
                poll_url = f"{workspace_url}api/2.0/genie/spaces/{genie_room_id}/conversations/{conversation_id}/messages/{message_id}"
                max_retries = 120  # 4 minutes of polling (with 2-second intervals)
                retry_interval = 2  # seconds

                for attempt in range(max_retries):
                    async with session.get(poll_url, headers=headers) as poll_response:
                        if poll_response.status != 200:
                            error_text = await poll_response.text()
                            logger.error(f"Error polling: Status {poll_response.status}, Response: {error_text}")
                            # Continue polling despite errors
                            await asyncio.sleep(retry_interval)
                            continue
                            
                        poll_text = await poll_response.text()
                        try:
                            poll_json = json.loads(poll_text)
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse JSON poll response: {poll_text}")
                            await asyncio.sleep(retry_interval)
                            continue

                    if "attachments" in poll_json and poll_json["attachments"]:
                        attachment = poll_json["attachments"][0]
                        attachment_id = attachment["attachment_id"]

                        answer_url = (
                            f"{workspace_url}api/2.0/genie/spaces/{genie_room_id}/conversations/"
                            f"{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
                        )

                        async with session.get(answer_url, headers=headers) as result_response:
                            if result_response.status != 200:
                                error_text = await result_response.text()
                                logger.error(f"Error getting result: Status {result_response.status}, Response: {error_text}")
                                break  # Break out of polling loop, will either retry or return error
                                
                            result_text = await result_response.text()
                            try:
                                result_json = json.loads(result_text)
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse JSON result: {result_text}")
                                break  # Break out of polling loop, will either retry or return error

                        state = result_json.get("statement_response", {}).get("status", {}).get("state")

                        if state == "SUCCEEDED":
                            logger.info("Query succeeded!")
                            # Include conversation_id in the successful response
                            result_json["conversation_id"] = conversation_id
                            return result_json
                        elif state == "FAILED":
                            logger.error(f"Query failed to execute (attempt {query_retry_count + 1}/{max_query_retries})")
                            if query_retry_count < max_query_retries - 1:
                                # Try again with the same conversation_id
                                query_retry_count += 1
                                # Add a short delay before retrying
                                await asyncio.sleep(2)
                                break  # Break out of polling loop and retry with same conversation_id
                            else:
                                # No more retries, return the failure
                                return {"error": "Query failed to execute after multiple attempts.", 
                                        "conversation_id": conversation_id,
                                        "attempts": query_retry_count + 1}
                        else:
                            logger.info(f"Query state: {state}, continuing to poll")
                    
                    # Log every 10 attempts
                    if attempt % 10 == 0:
                        logger.info(f"Polling attempt {attempt+1}/{max_retries}")
                        
                    await asyncio.sleep(retry_interval)
                
                # If we completed all polling attempts without success and haven't incremented retry_count
                # (meaning we didn't break out of the loop due to a FAILED state), then increment it now
                if query_retry_count < max_query_retries - 1:
                    query_retry_count += 1
                else:
                    # We've exhausted all retries
                    break
            
            logger.warning(f"Failed to get result after {query_retry_count + 1} attempts")
            return {"error": f"Could not get query result after {query_retry_count + 1} attempts.", 
                    "conversation_id": conversation_id,
                    "attempts": query_retry_count + 1}
            
        except aiohttp.ClientError as e:
            logger.exception(f"HTTP error in fetch_answer: {e}")
            return {"error": f"Network error: {str(e)}", "conversation_id": conversation_id}
        except Exception as e:
            logger.exception(f"Unexpected error in fetch_answer: {e}")
            return {"error": f"Unexpected error: {str(e)}", "conversation_id": conversation_id}
