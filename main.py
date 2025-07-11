import logging
import re
import requests
import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import uuid
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import csv
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from input_json_AS import AS_input_json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

conversations = {}

DATABRICKS_ENDPOINT = "https://adb-574728181281554.14.azuredatabricks.net/serving-endpoints/appointment-scheduler/invocations"
headers = {
    "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN', 'dapi316d67f10aed42d262b7d7d0d13dae4f-3')}",
    "Content-Type": "application/json"
}

BLOB_CONN_STR = "DefaultEndpointsProtocol=https;AccountName=aitoolschatbotssa;AccountKey=wRiWLbBUPKodq8CTuhe4FeItgqdWZ45+DJZKNWY6FBeFbMBpvmZLb5W9FolclFZy6QKnrKcPP9lr+AStCnPrGQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER = "appointment"
BLOB_CSV_PATH = "appointments_saved_bookings.csv/appointments"

class ChatInput(BaseModel):
    conversation_id: str
    user_input: str

class ChatResponse(BaseModel):
    message: str
    state: str
    departments: Optional[List[str]] = None
    branches: Optional[List[Dict[str, str]]] = None
    doctors: Optional[List[Dict[str, str]]] = None
    genders: Optional[List[str]] = None
    conversation_ended: bool = False
    selected_date: Optional[str] = None

def get_base_payload():
    return AS_input_json.copy()

def send_databricks_request(endpoint, payload, headers, retries=3, backoff_factor=0.5):
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    logger.debug(f"Sending request to {endpoint} with payload keys: {list(payload.get('inputs', {}).keys())}")
    try:
        response = session.post(endpoint, json=payload, headers=headers, timeout=120)
        logger.debug(f"Response status: {response.status_code}")
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None
    finally:
        session.close()

def parse_top_doctors(top_doctors_str):
    doctors = []
    blocks = top_doctors_str.strip().split('\n\n')
    for block in blocks:
        lines = block.split('\n')
        if not lines:
            continue
        match = re.match(r'\d+\.\s*Dr\.\s*(.*)\s*\(ID:\s*(DOC\d+)\)\s*-\s*Branch:\s*(.*?)\s*-\s*Department:\s*(.*)', lines[0].strip())
        if match:
            doctor = {
                'Doctor_Name': match.group(1).strip(),
                'Doctor_ID': match.group(2).strip(),
                'Branch': match.group(3).strip(),
                'Specialization': match.group(4).strip(),
                'Available_Date': '',
                'Time_Slot': ''
            }
            for line in lines[1:]:
                line = line.strip()
                if line.startswith('Available Date:'):
                    doctor['Available_Date'] = line.split(':', 1)[1].strip()
                elif line.startswith('Available Time Slot:'):
                    doctor['Time_Slot'] = line.split(':', 1)[1].strip()
            doctors.append(doctor)
    return doctors

def save_appointment_to_adls(appointment_data: dict):
    service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container = service.get_container_client(BLOB_CONTAINER)
    blob = container.get_blob_client(BLOB_CSV_PATH)
    try:
        existing_bytes = blob.download_blob().readall()
        existing_text = existing_bytes.decode("utf-8")
    except ResourceNotFoundError:
        existing_text = ""
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(appointment_data.keys()))
    if not existing_text:
        writer.writeheader()
    writer.writerow(appointment_data)
    combined = existing_text + output.getvalue()
    blob.upload_blob(combined.encode("utf-8"), overwrite=True)

def send_appointment_email(appointment_data):
    try:
        msg = MIMEMultipart()
        msg['From'] = "ankura@hospital.com"
        msg['To'] = appointment_data['Email']
        msg['Subject'] = f"Appointment Confirmation - ID: {appointment_data['Appointment_ID']}"
        body = f"""
        Dear {appointment_data['Patient_Name']},
        Your appointment has been successfully booked. Below are the details:
        Appointment ID: {appointment_data['Appointment_ID']}
        Doctor: Dr. {appointment_data['Doctor_Name']}
        Department: {appointment_data['Department']}
        Branch: {appointment_data['Branch']}
        Date: {appointment_data['Selected_Date']}
        Time Slot: {appointment_data['Available_Time_Slot']}
        Booking Timestamp: {appointment_data['Booking_Timestamp']}
        Best regards,
        Healthcare Team
        """
        msg.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(os.getenv("ASemail"), os.getenv("ASappwd"))
            server.send_message(msg)
        logger.debug(f"Email sent to {appointment_data['Email']}")
        return True
    except Exception as e:
        logger.error(f"Email failed: {e}")
        return False

def fetch_summary_and_proceed(conversation_id, chat_history, symptom):
    state = conversations[conversation_id]
    base_payload = get_base_payload()
    base_payload.update({
        "task": "summarize_symptom",
        "pincode": state['personal_details']['pin_code'],
        "symptom": symptom,
        "age": state['personal_details']['age'],
        "gender": state['personal_details']['gender'],
        "followup_answers": [
            {'question': q, 'answer': a} for q, a in state.get('dynamic_followup_answers', {}).items()
        ],
        "raw_text": symptom,
        "appointment_data": {"Email": state['personal_details']['email']}
    })
    payload = {"inputs": base_payload}

    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    if response and response.status_code == 200:
        response_data = response.json()
        logger.debug(f"Summarize response: {response_data}")
        predictions = response_data.get('predictions', {})
        summary = predictions.get('summary', 'No summary provided.')

        base_payload = get_base_payload()
        base_payload.update({
            "task": "map_to_department",
            "summary": summary
        })
        payload = {"inputs": base_payload}
        response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
        if response and response.status_code == 200:
            response_data = response.json()
            logger.debug(f"Map to department response: {response_data}")
            departments = response_data.get('predictions', {}).get('departments', [])
            if departments:
                state['departments'] = departments
                state['personal_details']['summary'] = summary

                if departments and departments[0].lower() == "critical care / emergency medicine":
                    base_payload = get_base_payload()
                    base_payload.update({
                        "task": "find_nearest_branches",
                        "pincode": state['personal_details']['pin_code'],
                        "departments": departments,
                        "return_all": False
                    })
                    payload = {"inputs": base_payload}
                    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
                    if response and response.status_code == 200:
                        response_data = response.json()
                        branches = response_data.get('predictions', {}).get('branches', [])[:2]
                        logger.debug(f"Emergency branches: {branches}")
                        if branches:
                            branch_list = ", ".join([b['Branch'] for b in branches])
                            bot_message = (
                                f"This appears to be an emergency. Please call our ambulance service at 108 immediately. "
                                f"The two nearest hospital branches are: {branch_list}. Please rush to one of these hospitals. "
                                "Thank you for using our service."
                            )
                        else:
                            bot_message = (
                                "This appears to Appointment Confirmation - ID: {appointment_data['Appointment_ID']}"
                                "This appears to be an emergency. Please call our ambulance service at 108 immediately. "
                                "No branches found for your pincode. Please seek immediate medical attention."
                            )
                    else:
                        logger.error(f"Failed to fetch branches for emergency: {response.text if response else 'No response'}")
                        bot_message = (
                            "This appears to be an emergency. Please call our ambulance service at 108 immediately. "
                            "Unable to fetch hospital branches. Please seek immediate medical attention."
                        )
                    chat_history.append({'sender': 'bot', 'message': bot_message})
                    state['state'] = 'end'
                    conversations[conversation_id] = state
                    return ChatResponse(message=bot_message, state='end', conversation_ended=True)
                else:
                    if len(departments) > 1:
                        state['state'] = 'select_department'
                        bot_message = f"Based on your symptoms, please select a department: {', '.join(departments)}"
                        chat_history.append({'sender': 'bot', 'message': bot_message})
                        conversations[conversation_id] = state
                        return ChatResponse(message=bot_message, state='select_department', departments=departments)
                    else:
                        state['selected_department'] = departments[0]
                        state['state'] = 'confirm_appointment'
                        bot_message = "Do you want to book an appointment with this department?"
                        chat_history.append({'sender': 'bot', 'message': bot_message})
                        conversations[conversation_id] = state
                        return ChatResponse(message=bot_message, state='confirm_appointment', departments=departments)
        logger.error(f"Failed to map to departments: {response.text if response else 'No response'}")
    else:
        logger.error(f"Failed to summarize symptoms: {response.text if response else 'No response'}")

    bot_message = "Failed to process symptoms. Please try again."
    chat_history.append({'sender': 'bot', 'message': bot_message})
    state['state'] = 'ask_symptoms'
    conversations[conversation_id] = state
    return ChatResponse(message=bot_message, state='ask_symptoms')

def find_nearest_branches(conversation_id, chat_history, return_all=False):
    state = conversations[conversation_id]
    base_payload = get_base_payload()
    base_payload.update({
        "task": "find_nearest_branches",
        "pincode": state['personal_details']['pin_code'],
        "departments": [state.get('selected_department')] if state.get('selected_department') else state['departments'],
        "return_all": return_all
    })
    payload = {"inputs": base_payload}
    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    if response and response.status_code == 200:
        response_data = response.json()
        branches = response_data.get('predictions', {}).get('branches', [])
        logger.debug(f"Branches returned (return_all={return_all}): {branches}")
        if branches:
            # Ensure Pincode is string for state storage
            formatted_branches = [
                {
                    'pin_code': str(branch['Pincode']),
                    'Branch': branch['Branch'],
                    'distance': str(branch['distance'])
                }
                for branch in branches
            ]
            state['branches'] = formatted_branches
            if not return_all:
                nearest_two = formatted_branches[:2]
                branch_list = "\n".join([f"{i+1}. {b['Branch']}" for i, b in enumerate(nearest_two)])
                bot_message = f"The two nearest branches are:\n{branch_list}\nDo you want to proceed with these or see more?"
                state['state'] = 'confirm_branches'
                chat_history.append({'sender': 'bot', 'message': bot_message})
                conversations[conversation_id] = state
                return ChatResponse(message=bot_message, state='confirm_branches', branches=nearest_two)
            else:
                branch_options = "\n".join([f"{i+1}. {b['Branch']}" for i, b in enumerate(formatted_branches)])
                bot_message = f"Here are all available branches:\n{branch_options}\nPlease select branches by typing their numbers separated by commas."
                state['state'] = 'select_branches'
                chat_history.append({'sender': 'bot', 'message': bot_message})
                conversations[conversation_id] = state
                return ChatResponse(message=bot_message, state='select_branches', branches=formatted_branches)
        bot_message = "No branches found for your pincode. Please enter a different pincode."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_pincode'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_pincode')
    logger.error(f"Failed to find nearest branches: {response.text if response else 'No response'}")
    bot_message = "Failed to fetch branches. Please try again."
    chat_history.append({'sender': 'bot', 'message': bot_message})
    state['state'] = 'ask_appointment_date'
    conversations[conversation_id] = state
    return ChatResponse(message=bot_message, state='ask_appointment_date')

def filter_doctors(conversation_id: str, chat_history: List[Dict[str, str]]) -> ChatResponse:
    """
    Filter doctors and validate appointment date for the Android app.

    Args:
        conversation_id (str): Unique identifier for the conversation.
        chat_history (List[Dict[str, str]]): List of chat messages.

    Returns:
        ChatResponse: Response object with message and state.
    """
    state = conversations[conversation_id]
    selected_department = state.get('selected_department')
    if not selected_department:
        logger.error("No selected department found in conversation state")
        bot_message = "No department selected. Please start over."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_symptoms'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_symptoms')

    selected_date = state['selected_date']

    # Transform branches to ensure 'Pincode' is an integer
    transformed_branches = [
        {
            'Pincode': int(branch.get('pin_code', branch.get('Pincode'))),
            'Branch': branch['Branch'],
            'distance': float(branch['distance'])
        }
        for branch in state['selected_branches']
    ]

    # Step 0: Validate appointment date with extended payload
    base_payload = get_base_payload()
    base_payload.update({
        "task": "validate_appointment_date",
        "selected_date": selected_date,
        "departments": [selected_department],
        "branches": transformed_branches,
        "pincode": state['personal_details']['pin_code'],
        "symptom": state['personal_details']['symptom'],
        "age": state['personal_details']['age'],
        "gender": state['personal_details']['gender'],
        "summary": state['personal_details']['summary'],
        "appointment_data": {"Email": state['personal_details']['email']}
    })
    payload = {"inputs": base_payload}
    logger.debug(f"Step 0: Sending extended payload - {json.dumps(payload, indent=2)}")
    
    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    
    if response is None:
        logger.error("Step 0: No response from Databricks endpoint - possible network or server issue")
        bot_message = "Unable to connect to the server to validate the date. Please try again later."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_appointment_date'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_appointment_date')
    
    if response.status_code == 200:
        try:
            response_data = response.json()
            is_valid_date = response_data.get('predictions', {}).get('valid', False)
            logger.debug(f"Step 0: Date valid: {is_valid_date}")
            if not is_valid_date:
                bot_message = "The selected date is invalid or not available. Please choose a different date."
                chat_history.append({'sender': 'bot', 'message': bot_message})
                state['state'] = 'ask_appointment_date'
                conversations[conversation_id] = state
                return ChatResponse(message=bot_message, state='ask_appointment_date')
        except ValueError as e:
            logger.error(f"Step 0: Failed to parse JSON response - {str(e)}")
            bot_message = "Server error while validating date. Please try again."
            chat_history.append({'sender': 'bot', 'message': bot_message})
            state['state'] = 'ask_appointment_date'
            conversations[conversation_id] = state
            return ChatResponse(message=bot_message, state='ask_appointment_date')
    else:
        logger.error(f"Step 0: Validation failed - Status: {response.status_code}, Response: {response.text if response.text else 'No error message returned'}")
        bot_message = f"Unable to validate the appointment date. Error: {response.text[:100] if response.text else 'No details provided by server'}. Please try again."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_appointment_date'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_appointment_date')

    # Step 1: Recommend available doctors with relevant context
    base_payload = get_base_payload()
    base_payload.update({
        "task": "recommend_available_doctors_with_visit_reason_summary",
        "departments": [selected_department],
        "branches": transformed_branches,
        "selected_date": selected_date,
        "visit_reason_summary": state['personal_details']['summary'],
        "pincode": state['personal_details']['pin_code'],
        "symptom": state['personal_details']['symptom'],
        "age": state['personal_details']['age'],
        "gender": state['personal_details']['gender'],
        "appointment_data": {"Email": state['personal_details']['email']}
    })
    payload = {"inputs": base_payload}
    logger.debug(f"Step 1: Sending payload - {json.dumps(payload, indent=2)}")
    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    final_dr_list = []
    grouped_text = ""
    if response and response.status_code == 200:
        response_data = response.json()
        final_dr_list = response_data.get('predictions', {}).get('final_dr_list', [])
        grouped_text = response_data.get('predictions', {}).get('grouped_text', "")
        logger.debug(f"Step 1: Final doctors list count: {len(final_dr_list)}, Grouped text: {grouped_text}")
        if not final_dr_list:
            bot_message = "No doctors available for the selected date and department. Please try a different date."
            chat_history.append({'sender': 'bot', 'message': bot_message})
            state['state'] = 'ask_appointment_date'
            conversations[conversation_id] = state
            return ChatResponse(message=bot_message, state='ask_appointment_date')
    else:
        try:
            error_details = response.json() if response.text else {"message": "No details"}
            logger.error(f"Step 1: Failed - Status: {response.status_code}, Error: {error_details}")
        except ValueError:
            logger.error(f"Step 1: Failed - Status: {response.status_code}, Response: {response.text}")
        bot_message = f"Unable to find doctors. Error: {error_details.get('message', response.text[:100])}, please try a different date or contact support."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_appointment_date'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_appointment_date')

    # Step 6: Map to similar cases based on grouped text and summary
    base_payload = get_base_payload()
    base_payload.update({
        "task": "llm_maps_to_similar_cases",
        "grouped_text": grouped_text,
        "summary": state['personal_details']['summary'],
        "branches": transformed_branches
    })
    payload = {"inputs": base_payload}
    logger.debug(f"Step 6: Sending payload - {json.dumps(payload, indent=2)}")
    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    similar_cases = []
    raw_text = ""
    if response and response.status_code == 200:
        response_data = response.json()
        similar_cases = response_data.get('predictions', {}).get('doctor_ids_ordered', [])
        raw_text = response_data.get('predictions', {}).get('raw_text', "")
        logger.debug(f"Step 6: Similar cases count: {len(similar_cases)}, Raw text: {raw_text}")
        if not similar_cases:
            bot_message = "No similar cases found for your symptoms. Try a different symptom or date."
            chat_history.append({'sender': 'bot', 'message': bot_message})
            state['state'] = 'ask_appointment_date'
            conversations[conversation_id] = state
            return ChatResponse(message=bot_message, state='ask_appointment_date')
    else:
        try:
            error_details = response.json() if response.text else {"message": "No details"}
            logger.error(f"Step 6: Failed - Status: {response.status_code}, Error: {error_details}")
        except ValueError:
            logger.error(f"Step 6: Failed - Status: {response.status_code}, Response: {response.text}")
        bot_message = f"Unable to map to similar cases. Error: {error_details.get('message', response.text[:100])}, try a different date or contact support."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_appointment_date'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_appointment_date')

    # Step 7: Get top 3 doctors and blocks
    base_payload = get_base_payload()
    base_payload.update({
        "task": "top3_and_blocks",
        "final_dr_list": final_dr_list,
        "doctor_ids_ordered": similar_cases,
        "selected_date": selected_date,
        "raw_text": raw_text
    })
    payload = {"inputs": base_payload}
    logger.debug(f"Step 7: Sending payload - {json.dumps(payload, indent=2)}")
    response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
    if response and response.status_code == 200:
        response_data = response.json()
        top_doctors = response_data.get('predictions', {}).get('recommended_doctors', [])
        logger.debug(f"Step 7: Top doctors response: {top_doctors}")
        if isinstance(top_doctors, str):
            final_doctors_list = parse_top_doctors(top_doctors)
        else:
            final_doctors_list = top_doctors
        
        # Filter doctors by selected department to ensure relevance
        final_doctors_list = [
            doctor for doctor in final_doctors_list
            if doctor['Specialization'] == selected_department
        ]

        if final_doctors_list:
            # Format doctor list for user display
            doctor_options = "\n".join([
                f"{i+1}. Dr. {doctor['Doctor_Name']} (ID: {doctor['Doctor_ID']}) - Branch: {doctor['Branch']} - Department: {doctor['Specialization']} - Available Time: {doctor['Time_Slot']}"
                for i, doctor in enumerate(final_doctors_list)
            ])
            bot_message = f"Here are the recommended doctors for your appointment:\n{doctor_options}\nPlease select a doctor by typing their number."
            state['state'] = 'select_doctor'
            state['available_doctors'] = final_doctors_list
            chat_history.append({'sender': 'bot', 'message': bot_message})
            conversations[conversation_id] = state
            return ChatResponse(message=bot_message, state='select_doctor', doctors=final_doctors_list, selected_date=selected_date)
        else:
            bot_message = f"No doctors found for {selected_department} on the selected date. Please try a different date or department."
            logger.error(f"No doctors found for department: {selected_department}, Top doctors: {top_doctors}")
            chat_history.append({'sender': 'bot', 'message': bot_message})
            state['state'] = 'ask_appointment_date'
            conversations[conversation_id] = state
            return ChatResponse(message=bot_message, state='ask_appointment_date')
    else:
        try:
            error_details = response.json() if response.text else {"message": "No details"}
            logger.error(f"Step 7: Failed - Status: {response.status_code}, Error: {error_details}")
        except ValueError:
            logger.error(f"Step 7: Failed - Status: {response.status_code}, Response: {response.text}")
        bot_message = f"Unable to rank doctors. Error: {error_details.get('message', response.text[:100])}, try a different date or contact support."
        chat_history.append({'sender': 'bot', 'message': bot_message})
        state['state'] = 'ask_appointment_date'
        conversations[conversation_id] = state
        return ChatResponse(message=bot_message, state='ask_appointment_date')

@app.post("/start")
def start_conversation():
    conversation_id = str(uuid.uuid4())
    state = {
        'state': 'ask_name',
        'personal_details': {'name': '', 'age': 0, 'gender': '', 'pin_code': '', 'email': '', 'symptom': '', 'summary': ''},
        'chat_history': [],
        'followup_questions': [],
        'dynamic_followup_answers': {},
        'current_question_index': 0,
        'departments': [],
        'selected_department': '',
        'selected_date': '',
        'branches': [],
        'selected_branches': [],
        'available_doctors': [],
        'selected_doctor': {}
    }
    conversations[conversation_id] = state
    return {"conversation_id": conversation_id, "message": "Please provide your name.", "state": "ask_name"}

@app.post("/chatbot", response_model=ChatResponse)
def chat(input: ChatInput):
    conversation_id = input.conversation_id
    user_input = input.user_input.strip()
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")
    state = conversations[conversation_id]
    current_state = state['state']
    chat_history = state['chat_history']
    if user_input:
        chat_history.append({'sender': 'user', 'message': user_input})

    if current_state == 'ask_name':
        if not user_input:
            response_message = 'Please provide your name.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_name')
        state['personal_details']['name'] = user_input
        state['state'] = 'ask_age'
        response_message = 'Please provide your age.'
        chat_history.append({'sender': 'bot', 'message': response_message})
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_age')

    elif current_state == 'ask_age':
        if not user_input.isdigit() or int(user_input) <= 0:
            response_message = 'Please provide a valid age.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_age')
        state['personal_details']['age'] = int(user_input)
        state['state'] = 'ask_email'
        response_message = 'Please provide your email address.'
        chat_history.append({'sender': 'bot', 'message': response_message})
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_email')

    elif current_state == 'ask_email':
        if not re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', user_input):
            response_message = 'Please provide a valid email address.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_email')
        state['personal_details']['email'] = user_input
        state['state'] = 'ask_gender'
        response_message = 'Please select your gender.'
        chat_history.append({'sender': 'bot', 'message': response_message})
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_gender', genders=['Male', 'Female', 'Other'])

    elif current_state == 'ask_gender':
        valid_genders = ['Male', 'Female', 'Other']
        if user_input not in valid_genders:
            response_message = 'Please select a valid gender from: Male, Female, Other.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_gender', genders=valid_genders)
        state['personal_details']['gender'] = user_input
        state['state'] = 'ask_pincode'
        response_message = 'Please provide your pin code.'
        chat_history.append({'sender': 'bot', 'message': response_message})
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_pincode')

    elif current_state == 'ask_pincode':
        if not user_input.isdigit() or len(user_input) != 6:
            response_message = 'Please provide a valid 6-digit pin code.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_pincode')
        state['personal_details']['pin_code'] = user_input
        state['state'] = 'ask_symptoms'
        response_message = 'Please describe your symptoms.'
        chat_history.append({'sender': 'bot', 'message': response_message})
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_symptoms')

    elif current_state == 'ask_symptoms':
        if not user_input:
            response_message = 'Please describe your symptoms.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_symptoms')
        state['personal_details']['symptom'] = user_input

        base_payload = get_base_payload()
        base_payload.update({
            "task": "get_followup_questions",
            "pincode": state['personal_details']['pin_code'],
            "symptom": user_input,
            "age": state['personal_details']['age'],
            "gender": state['personal_details']['gender'],
            "raw_text": user_input,
            "appointment_data": {"Email": state['personal_details']['email']}
        })
        payload = {"inputs": base_payload}

        response = send_databricks_request(DATABRICKS_ENDPOINT, payload, headers)
        if response and response.status_code == 200:
            response_data = response.json()
            logger.debug(f"Follow-up questions response: {response_data}")
            questions = response_data.get('predictions', {}).get('followup_questions', [])
            followup_questions = [re.sub(r'\*\*', '', re.search(r'^.*?\?', q).group(0)) for q in questions]
            if followup_questions:
                state['followup_questions'] = followup_questions
                state['current_question_index'] = 0
                state['dynamic_followup_answers'] = {}
                state['state'] = 'ask_followup'
                response_message = followup_questions[0]
                chat_history.append({'sender': 'bot', 'message': response_message})
                conversations[conversation_id] = state
                return ChatResponse(message=response_message, state='ask_followup')
            return fetch_summary_and_proceed(conversation_id, chat_history, user_input)
        logger.error(f"Failed to get follow-up questions: {response.text if response else 'No response'}")
        response_message = "Failed to process symptoms. Please try again."
        chat_history.append({'sender': 'bot', 'message': response_message})
        state['state'] = 'ask_symptoms'
        conversations[conversation_id] = state
        return ChatResponse(message=response_message, state='ask_symptoms')

    elif current_state == 'ask_followup':
        if not user_input:
            response_message = 'Please provide an answer.'
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_followup')
        followup_questions = state.get('followup_questions', [])
        current_question_index = state.get('current_question_index', 0)
        dynamic_followup_answers = state.get('dynamic_followup_answers', {})

        if followup_questions and current_question_index < len(followup_questions):
            current_question = followup_questions[current_question_index]
            dynamic_followup_answers[current_question] = user_input
            state['dynamic_followup_answers'] = dynamic_followup_answers
            current_question_index += 1
            state['current_question_index'] = current_question_index

            if current_question_index < len(followup_questions):
                state['state'] = 'ask_followup'
                response_message = followup_questions[current_question_index]
                chat_history.append({'sender': 'bot', 'message': response_message})
                conversations[conversation_id] = state
                return ChatResponse(message=response_message, state='ask_followup')
            symptom = state['personal_details']['symptom']
            combined_symptom = f"{symptom} {json.dumps([{'question': q, 'answer': a} for q, a in dynamic_followup_answers.items()])}" if dynamic_followup_answers else symptom
            return fetch_summary_and_proceed(conversation_id, chat_history, combined_symptom)
        return fetch_summary_and_proceed(conversation_id, chat_history, state['personal_details']['symptom'])

    elif current_state == 'select_department':
        departments = state['departments']
        if user_input in departments:
            state['selected_department'] = user_input
            state['state'] = 'confirm_appointment'
            response_message = "Do you want to book an appointment with this department?"
            chat_history.append({'sender': 'bot', 'message': response_message})
            conversations[conversation_id] = state
            return ChatResponse(message=response_message, state='confirm_appointment')
        response_message = f"Invalid selection. Please select from: {', '.join(departments)}"
        chat_history.append({'sender': 'bot', 'message': response_message})
        return ChatResponse(message=response_message, state='select_department', departments=departments)

    elif current_state == 'confirm_appointment':
        if user_input.lower() in ['yes', 'y']:
            state['state'] = 'ask_appointment_date'
            response_message = "Please select your preferred appointment date (within the next month, format: YYYY-MM-DD)."
            chat_history.append({'sender': 'bot', 'message': response_message})
            conversations[conversation_id] = state
            return ChatResponse(message=response_message, state='ask_appointment_date')
        elif user_input.lower() in ['no', 'n']:
            response_message = "Thank you for using our service. Have a great day!"
            chat_history.append({'sender': 'bot', 'message': response_message})
            state['state'] = 'end'
            conversations[conversation_id] = state
            return ChatResponse(message=response_message, state='end', conversation_ended=True)
        response_message = "Please select Yes or No."
        chat_history.append({'sender': 'bot', 'message': response_message})
        return ChatResponse(message=response_message, state='confirm_appointment')

    elif current_state == 'ask_appointment_date':
        try:
            selected_date = datetime.strptime(user_input, '%Y-%m-%d').date()
            today = datetime.now().date()
            one_month_later = today + timedelta(days=30)
            if selected_date <= today or selected_date > one_month_later:
                raise ValueError
            state['selected_date'] = selected_date.strftime('%Y-%m-%d')
            return find_nearest_branches(conversation_id, chat_history, return_all=False)
        except ValueError:
            response_message = "Invalid date. Please select a future date within one month (format: YYYY-MM-DD)."
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='ask_appointment_date')

    elif current_state == 'confirm_branches':
        if user_input and user_input.lower() in ['proceed', 'yes', 'y']:
            state['selected_branches'] = state['branches'][:2]
            return filter_doctors(conversation_id, chat_history)
        elif user_input.lower() in ['see more', 'more']:
            return find_nearest_branches(conversation_id, chat_history, return_all=True)
        response_message = "Please respond with 'Proceed' or 'See more'."
        chat_history.append({'sender': 'bot', 'message': response_message})
        return ChatResponse(message=response_message, state='confirm_branches', branches=state['branches'][:2])

    elif current_state == 'select_branches':
        try:
            if not user_input:
                raise ValueError("No input provided")
            selected_indices = [int(idx.strip()) - 1 for idx in user_input.split(',') if idx.strip().isdigit()]
            all_branches = state.get('branches', [])
            if not all_branches:
                response_message = "No branches available. Please try again."
                chat_history.append({'sender': 'bot', 'message': response_message})
                return ChatResponse(message=response_message, state='ask_appointment_date')
            selected_branches = [all_branches[i] for i in selected_indices if 0 <= i < len(all_branches)]
            if not selected_branches:
                raise ValueError("No valid branches selected")
            state['selected_branches'] = selected_branches
            return filter_doctors(conversation_id, chat_history)
        except ValueError as e:
            response_message = f"Invalid selection: {str(e)}. Please select branches by typing their numbers separated by commas."
            chat_history.append({'sender': 'bot', 'message': response_message})
            return ChatResponse(message=response_message, state='select_branches', branches=state.get('branches', []))

    elif current_state == 'select_doctor':
        try:
            selected_index = int(user_input.strip()) - 1
            available_doctors = state['available_doctors']
            if 0 <= selected_index < len(available_doctors):
                selected_doctor = available_doctors[selected_index]
                state['selected_doctor'] = selected_doctor
                appointment_id = f"APT-{uuid.uuid4().hex[:8]}"
                booking_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                appointment_data = {
                    "Patient_Name": state['personal_details']['name'],
                    "Age": str(state['personal_details']['age']),
                    "Gender": state['personal_details']['gender'],
                    "Pincode": state['personal_details']['pin_code'],
                    "Symptom_Summary": state['personal_details']['summary'],
                    "Department": state['selected_department'],
                    "Doctor_ID": selected_doctor.get("Doctor_ID", "N/A"),
                    "Doctor_Name": selected_doctor.get("Doctor_Name", "N/A"),
                    "Branch": selected_doctor['Branch'],
                    "Selected_Date": state['selected_date'],
                    "Available_Time_Slot": selected_doctor.get('Time_Slot', 'N/A'),
                    "Email": state['personal_details']['email'],
                    "Appointment_ID": appointment_id,
                    "Booking_Timestamp": booking_ts
                }

                if appointment_id:
                    bot_message = f"Appointment booked successfully with Dr. {appointment_data['Doctor_Name']} on {appointment_data['Selected_Date']} at {appointment_data['Available_Time_Slot']}! Your appointment ID is {appointment_id}."
                    chat_history.append({'sender': 'bot', 'message': bot_message})
                    save_appointment_to_adls(appointment_data)
                    email_sent = send_appointment_email(appointment_data)
                    if not email_sent:
                        bot_message += " However, we couldn't send a confirmation email. Please check your email address."
                        chat_history.append({'sender': 'bot', 'message': bot_message})
                    state['state'] = 'end'
                    conversations[conversation_id] = state
                    return ChatResponse(message=bot_message, state='end', conversation_ended=True)
                bot_message = "Failed to book appointment. Please try again."
                chat_history.append({'sender': 'bot', 'message': bot_message})
                state['state'] = 'end'
                conversations[conversation_id] = state
                return ChatResponse(message=bot_message, state='end')
            raise ValueError
        except ValueError:
            response_message = "Invalid selection. Please select a doctor by typing their number."
            chat_history.append({'sender': 'bot', 'message': response_message})
            state['state'] = 'select_doctor'
            conversations[conversation_id] = state
            return ChatResponse(message=response_message, state='select_doctor', doctors=state['available_doctors'], selected_date=state['selected_date'])

    response_message = "State not fully implemented. Please try again."
    chat_history.append({'sender': 'bot', 'message': response_message})
    conversations[conversation_id] = state
    return ChatResponse(message=response_message, state=current_state)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
