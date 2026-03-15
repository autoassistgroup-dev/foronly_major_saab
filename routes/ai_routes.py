"""
AI Routes Blueprint

This module contains API endpoints for AI-related functionality,
including AI response generation and display.

Author: AutoAssistGroup Development Team
"""

from flask import Blueprint, request, jsonify
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

ai_bp = Blueprint('ai', __name__, url_prefix='/api/ai')


def extract_ticket_id_from_body(body):
    """
    Extract existing ticket ID from email body.
    Looks for patterns like 'ticket #EE3295', 'Ticket ID: EE3295', etc.
    
    Returns the first matched ticket ID or None.
    """
    if not body:
        return None
    
    # Patterns to match ticket IDs in email body
    patterns = [
        r'ticket\s*#?\s*([A-Z]{1,3}\d{3,6})',  # ticket #EE3295, ticket EE3295
        r'ticket\s+id[:\s]+([A-Z]{1,3}\d{3,6})',  # Ticket ID: EE3295
        r'regarding\s+ticket\s*#?\s*([A-Z]{1,3}\d{3,6})',  # regarding ticket #EE3295
        r'#([A-Z]{1,3}\d{3,6})',  # Just #EE3295
    ]
    
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            ticket_id = match.group(1).upper()
            logger.info(f"Extracted ticket ID from body: {ticket_id}")
            return ticket_id
    
    return None


@ai_bp.route('/display-response', methods=['POST', 'GET'])
def display_response():
    """
    Display or verify AI response generation.
    
    GET: Test endpoint availability
    POST: Process and save AI response data to ticket
    """
    if request.method == 'GET':
        return jsonify({
            'success': True,
            'message': 'AI Display API is working',
            'timestamp': datetime.now().isoformat()
        })
    
    try:
        data = request.get_json() or {}
        
        original_ticket_id = data.get('ticket_id', '')
        ai_response = data.get('ai_response', data.get('draft', ''))
        email_body = data.get('body', data.get('message', ''))
        customer_email = data.get('from', data.get('customer_email', ''))
        
        # SMART TICKET ID EXTRACTION
        # First, try to extract real ticket ID from email body (for replies)
        extracted_ticket_id = extract_ticket_id_from_body(email_body)
        
        # Use extracted ID if found, otherwise use the one from N8N
        ticket_id = extracted_ticket_id or original_ticket_id
        
        if extracted_ticket_id and extracted_ticket_id != original_ticket_id:
            logger.info(f"Using extracted ticket ID {extracted_ticket_id} instead of AI-generated {original_ticket_id}")
        
        if ticket_id and ai_response:
            # Save the AI draft to the ticket in database
            from database import get_db
            db = get_db()
            
            # First, try to find the ticket by ticket_id
            ticket = db.get_ticket_by_id(ticket_id)
            
            # If ticket not found and we have original_ticket_id, try that too
            if not ticket and extracted_ticket_id and original_ticket_id:
                ticket = db.get_ticket_by_id(original_ticket_id)
                if ticket:
                    ticket_id = original_ticket_id
                    logger.info(f"Found ticket using original ID: {original_ticket_id}")
            
            # If still not found, try to find by customer email (for reply scenarios)
            if not ticket and customer_email:
                # Find the most recent ticket from this customer
                tickets = list(db.tickets.find(
                    {"email": customer_email}
                ).sort("created_at", -1).limit(1))
                if tickets:
                    ticket = tickets[0]
                    ticket_id = ticket.get('ticket_id')
                    logger.info(f"Found ticket {ticket_id} by customer email {customer_email}")
            
            if not ticket:
                logger.warning(f"Ticket {ticket_id} not found, creating update anyway")
            
            # Update the ticket with the AI draft
            result = db.update_ticket(ticket_id, {
                'draft': ai_response,
                'draft_body': ai_response,
                'n8n_draft': ai_response,
                'updated_at': datetime.now()
            })
            
            # Check if any document was actually modified
            updated_count = result.matched_count if hasattr(result, 'matched_count') else 0
            
            if updated_count == 0:
                logger.warning(f"No ticket found to update with ID {ticket_id}")
                return jsonify({
                    'success': False,
                    'message': f"Ticket {ticket_id} not found. Ensure 'from' (email) is sent for fallback lookup.",
                    'ticket_id': ticket_id,
                    'customer_reply_saved': False
                }), 404
            
            logger.info(f"✅ AI draft saved for ticket {ticket_id}: {len(ai_response)} chars")
            
            # ── FIX: Update truncated customer reply with full body ──
            # N8N sends full body here, but /webhook/reply may receive truncated text.
            # Find any recent reply with a shorter message and update it.
            if email_body and ticket:
                try:
                    from routes.webhook_routes import strip_email_quotes
                    from datetime import timedelta
                    
                    full_message = strip_email_quotes(email_body)
                    
                    # Extract attachments from payload (if N8N sends them)
                    raw_attachments = data.get('attachments', [])
                    if isinstance(raw_attachments, dict):
                        raw_attachments = list(raw_attachments.values())
                    payload_attachments = []
                    for att in (raw_attachments if isinstance(raw_attachments, list) else []):
                        if isinstance(att, dict):
                            if not att.get('filename'):
                                att['filename'] = att.get('fileName', 'attachment')
                            payload_attachments.append(att)
                    
                    if full_message and len(full_message) > 10:
                        # Find the most recent webhook reply for this ticket (within 5 min)
                        cutoff = datetime.now() - timedelta(minutes=5)
                        recent_reply = db.replies.find_one(
                            {
                                'ticket_id': ticket_id,
                                'created_at': {'$gte': cutoff},
                                'sender_type': 'webhook'
                            },
                            sort=[('created_at', -1)]
                        )
                        
                        if recent_reply:
                            update_fields = {}
                            existing_msg = recent_reply.get('message', '')
                            if len(full_message) > len(existing_msg):
                                update_fields['message'] = full_message
                            
                            # Also merge attachments if existing reply has none and we have some
                            existing_atts = recent_reply.get('attachments', [])
                            if not existing_atts and payload_attachments:
                                update_fields['attachments'] = payload_attachments
                                logger.info(f"📎 ATTACHMENTS RECEIVED │ Ticket {ticket_id} │ {len(payload_attachments)} attachments from AI draft payload")
                            
                            if update_fields:
                                db.replies.update_one(
                                    {'_id': recent_reply['_id']},
                                    {'$set': update_fields}
                                )
                                logger.info(f"📝 REPLY PATCHED │ Ticket {ticket_id} │ {len(existing_msg)} → {len(full_message)} chars │ Attachments: {len(recent_reply.get('attachments', []))} → {len(update_fields.get('attachments', recent_reply.get('attachments', [])))}")
                            else:
                                logger.info(f"📝 REPLY OK │ Ticket {ticket_id} │ Already has {len(existing_msg)} chars (body: {len(full_message)})")
                        else:
                            # No recent reply found — create one from the full body
                            sender_name = data.get('name', data.get('from', 'Customer'))
                            reply_data = {
                                'ticket_id': ticket_id,
                                'message': full_message,
                                'sender_name': sender_name,
                                'sender_type': 'webhook',
                                'attachments': payload_attachments,  # Include any attachments from payload
                                'created_at': datetime.now()
                            }
                            reply_id = db.create_reply(reply_data)
                            logger.info(f"📝 REPLY CREATED │ Ticket {ticket_id} │ {len(full_message)} chars │ Attachments: {len(payload_attachments)} │ ID: {reply_id}")
                except Exception as e:
                    logger.warning(f"⚠️ REPLY PATCH FAILED │ Ticket {ticket_id} │ {e}")
            
            return jsonify({
                'success': True,
                'message': 'AI response saved to ticket',
                'ticket_id': ticket_id,
                'draft': ai_response,
                'draft_length': len(ai_response),
                'customer_reply_saved': bool(email_body and customer_email and ticket),
                'timestamp': datetime.now().isoformat()
            })
        
        return jsonify({
            'success': True,
            'message': 'AI response received (no ticket_id provided)',
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in display_response: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@ai_bp.route('/get-response/<ticket_id>', methods=['GET'])
def get_ai_response(ticket_id):
    """
    Get AI-generated response for a specific ticket.
    
    This endpoint fetches the AI response that was saved by n8n via the
    /display-response endpoint, stored in the ticket's draft/n8n_draft fields.
    
    Args:
        ticket_id: The ticket ID to get AI response for
        
    Returns:
        JSON with AI response from database (saved by n8n)
    """
    try:
        from database import get_db
        db = get_db()
        
        # Fetch the ticket from database
        ticket = db.get_ticket_by_id(ticket_id)
        
        if not ticket:
            logger.warning(f"Ticket {ticket_id} not found when fetching AI response")
            return jsonify({
                'success': False,
                'ticket_id': ticket_id,
                'message': 'Ticket not found'
            }), 404
        
        # Get the AI response from database fields (saved by n8n via /display-response)
        # Priority: n8n_draft > draft_body > draft
        ai_response = (
            ticket.get('n8n_draft') or 
            ticket.get('draft_body') or 
            ticket.get('draft') or 
            ''
        )
        
        if ai_response:
            logger.info(f"Found AI response for ticket {ticket_id}: {len(ai_response)} chars")
            return jsonify({
                'success': True,
                'ticket_id': ticket_id,
                'ai_response': ai_response,
                'source': 'database',
                'generated_at': datetime.now().isoformat()
            })
        else:
            logger.info(f"No AI response found in database for ticket {ticket_id}")
            return jsonify({
                'success': False,
                'ticket_id': ticket_id,
                'ai_response': '',
                'message': 'No AI response available for this ticket'
            })
            
    except Exception as e:
        logger.error(f"Error fetching AI response for ticket {ticket_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@ai_bp.route('/health', methods=['GET'])
def ai_health():
    """Health check for AI service."""
    return jsonify({
        'status': 'healthy',
        'service': 'ai',
        'timestamp': datetime.now().isoformat()
    })
