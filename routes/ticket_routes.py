"""
Ticket API Routes

Handles all ticket CRUD operations including:
- Getting tickets (paginated, filtered)
- Creating tickets
- Updating ticket status
- Searching tickets
- Closing tickets

Author: AutoAssistGroup Development Team
"""

import logging
from datetime import datetime
from flask import Blueprint, jsonify, request, session

from middleware.session_manager import is_authenticated, safe_member_lookup
from utils.validators import sanitize_input, validate_ticket_id
from socket_events import (
    emit_new_ticket, emit_new_reply, emit_ticket_update,
    emit_status_changed, emit_priority_changed, emit_technician_assigned,
    emit_ticket_forwarded, emit_ticket_taken_over, emit_tech_director_referral,
    emit_bookmark_changed
)

logger = logging.getLogger(__name__)

# Create blueprint
ticket_bp = Blueprint('tickets', __name__, url_prefix='/api/tickets')


@ticket_bp.route('', methods=['GET'])
@ticket_bp.route('/', methods=['GET'])
def get_tickets():
    """
    Get paginated list of tickets with optional filters.
    
    Query params:
        page: Page number (default 1)
        per_page: Items per page (default 20)
        status: Filter by status
        priority: Filter by priority
        search: Search query
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        # Get pagination and filter params
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        status_filter = request.args.get('status')
        priority_filter = request.args.get('priority')
        search_query = request.args.get('search')
        
        # Validate per_page
        per_page = min(per_page, 100)  # Max 100 items per page
        
        from database import get_db
        db = get_db()
        
        # Get tickets with pagination
        tickets = db.get_tickets_with_assignments(
            page=page,
            per_page=per_page,
            status_filter=status_filter,
            priority_filter=priority_filter,
            search_query=search_query
        )
        
        # Forwarded tickets are handled by the index page's personalized
        # "Forwarded to You" section — no need to merge them here.
        
        # Get total count for pagination
        total = db.get_tickets_count(
            status_filter=status_filter,
            priority_filter=priority_filter,
            search_query=search_query
        )
        
        # Serialize tickets for JSON response
        serialized_tickets = []
        for ticket in tickets:
            serialized_tickets.append(_serialize_ticket(ticket))
        
        return jsonify({
            'success': True,
            'tickets': serialized_tickets,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': (total + per_page - 1) // per_page
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting tickets: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@ticket_bp.route('', methods=['POST'])
@ticket_bp.route('/', methods=['POST'])
def create_ticket_webhook():
    """
    Handle ticket creation from N8N webhook (or other API clients).
    Accepts JSON payload matching N8N structure.
    """
    try:
        # Check for JSON data
        if not request.is_json:
            return jsonify({'success': False, 'error': 'Content-Type must be application/json'}), 400
            
        data = request.get_json()
        
        # Reuse N8N processing logic
        from routes.n8n_routes import process_n8n_email_data
        
        processed = process_n8n_email_data(data)
        
        if not processed:
            return jsonify({'success': False, 'error': 'Invalid ticket data'}), 400
            
        from database import get_db
        db = get_db()
        
        # Create ticket
        ticket_id = db.create_ticket(processed)
        
        logger.info(f"Ticket created via webhook: {processed.get('ticket_id')}")
        
        # Emit real-time notification
        try:
            emit_new_ticket({
                'ticket_id': processed.get('ticket_id'),
                'subject': processed.get('subject', 'No Subject'),
                'name': processed.get('name', 'Anonymous'),
                'email': processed.get('email', ''),
                'priority': processed.get('priority', 'Medium'),
                'status': 'Open',
                'created_at': processed.get('created_at').isoformat() if processed.get('created_at') else None,
                'is_manual': False,
                'body': processed.get('body', '') or processed.get('description', '')
            })
        except Exception as e:
            logger.warning(f"Failed to emit new ticket event: {e}")

        return jsonify({
            'success': True, 
            'message': 'Ticket created successfully',
            'ticket_id': processed.get('ticket_id'),
            'db_id': str(ticket_id)
        })
        
    except ValueError as e:
        if "Thread ID already exists" in str(e):
            logger.warning(f"Duplicate thread ID detected via webhook: {e}")
            
            # Find the existing ticket
            from database import get_db
            db = get_db()
            
            thread_id = processed.get('thread_id')
            existing_ticket = db.tickets.find_one({"thread_id": thread_id})
            
            if existing_ticket:
                logger.info(f"Returning existing ticket {existing_ticket.get('ticket_id')} for thread {thread_id}")
                return jsonify({
                    'success': True, 
                    'message': 'Ticket already exists',
                    'ticket_id': existing_ticket.get('ticket_id'),
                    'db_id': str(existing_ticket.get('_id'))
                })
        
        logger.error(f"Error creating ticket via webhook: {e}")
        return jsonify({'success': False, 'error': str(e)}), 409
        
    except Exception as e:
        logger.error(f"Error creating ticket via webhook: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/create', methods=['POST'])
def create_ticket():
    """
    Create a new ticket via API.
    Handles AJAX form submission from create_ticket.html.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        current_member = safe_member_lookup()
        if not current_member:
            return jsonify({'success': False, 'error': 'User not found'}), 404
            
        import uuid
        from database import get_db
        db = get_db()
        
        # Generate ticket ID first to use in thread_id
        ticket_id = 'M' + str(uuid.uuid4())[:5].upper()
        
        # Extract form data
        customer_first_name = request.form.get('customer_first_name', '')
        customer_surname = request.form.get('customer_surname', '')
        customer_title = request.form.get('customer_title', '')
        
        # Build full customer name with optional title
        name_parts = []
        if customer_title:
            name_parts.append(customer_title)
        if customer_first_name:
            name_parts.append(customer_first_name)
        if customer_surname:
            name_parts.append(customer_surname)
        customer_full_name = ' '.join(name_parts).strip()
        
        ticket_data = {
            'ticket_id': ticket_id,
            'thread_id': f'manual_{ticket_id}',  # Ensure unique thread_id for database constraint
            'subject': request.form.get('subject', ''),
            'body': request.form.get('body', '') or request.form.get('description', ''),
            'description': request.form.get('description', ''), # Fallback or main?
            'customer_first_name': customer_first_name,
            'customer_surname': customer_surname,
            'customer_title': customer_title,
            'vehicle_registration': request.form.get('vehicle_registration', ''),
            'email': request.form.get('email', ''),
            'phone': request.form.get('phone', ''),
            'type_of_claim': request.form.get('type_of_claim', ''),
            'vhc_link': request.form.get('vhc_link', '').strip(),
            'status': 'New',
            'priority': request.form.get('priority', 'Medium'),
            'assigned_technician': request.form.get('technician', ''),
            'created_at': datetime.now(),
            'created_by': current_member.get('name', ''),
            'created_by_id': session.get('member_id'),
            'creation_method': 'api', 
            'is_forwarded': False
        }
        
        # Process attachments: persist to disk (no base64 in DB), same as n8n flow
        from config.settings import Config
        from utils.file_utils import save_attachment_bytes_to_disk, detect_warranty_form
        upload_root = Config.get_upload_folder()
        attachments = []
        has_warranty = False
        ticket_id = ticket_data['ticket_id']
        idx = 0
        
        def add_file_attachment(file_obj):
            nonlocal idx, has_warranty
            if not file_obj or not file_obj.filename:
                return
            try:
                file_bytes = file_obj.read()
                if not file_bytes:
                    return
                saved = save_attachment_bytes_to_disk(
                    upload_root, "tickets/" + str(ticket_id), f"ui_{idx}", file_obj.filename, file_bytes
                )
                if saved:
                    attachments.append({
                        "filename": saved["filename"],
                        "fileName": saved["filename"],
                        "file_path": saved["file_path"],
                        "mime_type": saved.get("mime_type", file_obj.content_type or "application/octet-stream"),
                        "size": saved["size"],
                    })
                    if detect_warranty_form(saved["filename"]):
                        has_warranty = True
                    idx += 1
            except Exception as e:
                logger.error(f"Error processing file {file_obj.filename}: {e}")
        
        if 'dpf_report' in request.files:
            add_file_attachment(request.files['dpf_report'])
        if 'warranty_form' in request.files:
            add_file_attachment(request.files['warranty_form'])
        if 'other_attachments' in request.files:
            for f in request.files.getlist('other_attachments'):
                add_file_attachment(f)

        ticket_data['attachments'] = attachments
        ticket_data['has_attachments'] = len(attachments) > 0
        ticket_data['total_attachments'] = len(attachments)
        ticket_data['has_warranty'] = has_warranty
        
        db.create_ticket(ticket_data)
        
        logger.info(f"Ticket {ticket_data['ticket_id']} created by {current_member.get('name')}")
        
        # Emit real-time notification
        try:
            emit_new_ticket({
                'ticket_id': ticket_data['ticket_id'],
                'subject': ticket_data.get('subject', 'No Subject'),
                'name': f"{ticket_data.get('customer_first_name', '')} {ticket_data.get('customer_surname', '')}".strip() or 'Anonymous',
                'email': ticket_data.get('email', ''),
                'priority': ticket_data.get('priority', 'Medium'),
                'status': 'New',
                'created_at': ticket_data.get('created_at').isoformat() if ticket_data.get('created_at') else None,
                'is_manual': True,
                'body': ticket_data.get('body', '')
            })
        except Exception as e:
            logger.warning(f"Failed to emit new ticket event: {e}")

        return jsonify({
            'status': 'success',
            'success': True,
            'message': 'Ticket created successfully',
            'ticket_id': ticket_data['ticket_id'],
            'customer_number': ticket_data['ticket_id']
        })
        
    except Exception as e:
        logger.error(f"Error creating ticket via API: {e}")
        return jsonify({'status': 'error', 'success': False, 'message': str(e)}), 500


@ticket_bp.route('/<ticket_id>', methods=['GET'])
def get_ticket(ticket_id):
    """Get a single ticket by ID."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        from database import get_db
        db = get_db()
        
        ticket = db.get_ticket_by_id(ticket_id)
        
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        return jsonify({
            'success': True,
            'ticket': _serialize_ticket(ticket)
        })
        
    except Exception as e:
        logger.error(f"Error getting ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/status', methods=['PUT', 'PATCH'])
def update_ticket_status(ticket_id):
    """
    Update ticket status.
    Available for ALL users including Technical Director.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        data = request.get_json()
        new_status = data.get('status')
        
        if not new_status:
            return jsonify({'success': False, 'error': 'Status is required'}), 400
        
        from database import get_db
        db = get_db()
        
        # Get existing ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        # Update status
        update_data = {
            'status': new_status,
            'updated_at': datetime.now()
        }
        
        db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} status updated to {new_status} by {session.get('member_name')}")
        
        # Emit real-time WebSocket event for status change
        # Emit real-time WebSocket event for status change
        try:
            emit_status_changed(ticket_id, {
                'ticket_id': ticket_id,
                'old_status': ticket.get('status'),
                'new_status': new_status,
                'changed_by_id': session.get('member_id'),
                'changed_by_name': session.get('member_name'),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit status change event: {e}")
        
        return jsonify({
            'success': True,
            'message': f'Status updated to {new_status}',
            'ticket_id': ticket_id
        })
        
    except Exception as e:
        logger.error(f"Error updating ticket status {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/close', methods=['POST'])
def close_ticket(ticket_id):
    """
    Close a ticket.
    Available for ALL users including Technical Director.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        from database import get_db
        db = get_db()
        
        # Get existing ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        # Update status to Closed
        update_data = {
            'status': 'Closed',
            'closed_at': datetime.now(),
            'closed_by': session.get('member_id'),
            'updated_at': datetime.now()
        }
        
        db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} closed by {session.get('member_name')}")
        
        return jsonify({
            'success': True,
            'message': 'Ticket closed successfully',
            'ticket_id': ticket_id
        })
        
    except Exception as e:
        logger.error(f"Error closing ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>', methods=['DELETE'])
def delete_ticket(ticket_id):
    """
    Delete a ticket permanently.
    Requires admin or authorized role.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        # Check if user is admin
        from middleware.session_manager import is_admin
        if not is_admin():
            return jsonify({'success': False, 'error': 'Admin access required'}), 403
        
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        from database import get_db
        db = get_db()
        
        # Check if ticket exists
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        # Delete the ticket
        result = db.tickets.delete_one({'ticket_id': ticket_id})
        
        if result.deleted_count > 0:
            logger.info(f"Ticket {ticket_id} deleted by {session.get('member_name')}")
            return jsonify({
                'success': True,
                'message': 'Ticket deleted successfully',
                'ticket_id': ticket_id
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to delete ticket'}), 500
        
    except Exception as e:
        logger.error(f"Error deleting ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/bulk-delete', methods=['POST'])
def bulk_delete_tickets():
    """
    Delete multiple tickets at once.
    Expects JSON body: {"ticket_ids": ["ID1", "ID2", ...]}
    Requires admin role.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from middleware.session_manager import is_admin
        if not is_admin():
            return jsonify({'success': False, 'error': 'Admin access required'}), 403
        
        data = request.get_json()
        ticket_ids = data.get('ticket_ids', [])
        
        if not ticket_ids or not isinstance(ticket_ids, list):
            return jsonify({'success': False, 'error': 'No ticket IDs provided'}), 400
        
        from database import get_db
        db = get_db()
        
        # Delete all matching tickets and their replies
        result = db.tickets.delete_many({'ticket_id': {'$in': ticket_ids}})
        db.replies.delete_many({'ticket_id': {'$in': ticket_ids}})
        
        deleted_count = result.deleted_count
        logger.info(f"Bulk deleted {deleted_count} tickets by {session.get('member_name')}: {ticket_ids}")
        
        return jsonify({
            'success': True,
            'message': f'{deleted_count} tickets deleted successfully',
            'deleted_count': deleted_count
        })
        
    except Exception as e:
        logger.error(f"Error bulk deleting tickets: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/reply', methods=['POST'])
def send_ticket_reply(ticket_id):
    """
    Send a reply to a ticket.
    Creates a reply record and optionally sends email to customer.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'message': 'Authentication required'}), 401
        
        from database import get_db
        db = get_db()
        
        # Get ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            return jsonify({'success': False, 'message': 'Ticket not found'}), 404
        
        # Handle multipart form data (with attachments) or JSON
        if request.content_type and 'multipart/form-data' in request.content_type:
            message = request.form.get('response_text',
                      request.form.get('response',
                      request.form.get('message', '')))
            send_email = request.form.get('sendEmail', 'false').lower() == 'true'
            
            # File attachments: persist to disk, store file_path in reply (no base64)
            attachments = []
            from config.settings import Config
            from utils.file_utils import save_attachment_bytes_to_disk
            upload_root = Config.get_upload_folder()
            reply_prefix = f"reply_{ticket_id}_{int(datetime.now().timestamp())}"
            for key in sorted(request.files.keys()):
                if key.startswith('attachment_') or key in ('attachments', 'response_attachments'):
                    files = request.files.getlist(key) if key in ('attachments', 'response_attachments') else [request.files[key]]
                    for idx, f in enumerate(files):
                        if f.filename:
                            file_bytes = f.read()
                            if not file_bytes:
                                continue
                            saved = save_attachment_bytes_to_disk(
                                upload_root, "replies", f"{reply_prefix}_{len(attachments)}", f.filename, file_bytes
                            )
                            if saved:
                                attachments.append({
                                    'filename': saved['filename'],
                                    'fileName': saved['filename'],
                                    'type': 'file',
                                    'file_path': saved['file_path'],
                                    'content_type': saved.get('mime_type', f.content_type or 'application/octet-stream'),
                                    'size': saved['size'],
                                })
                            else:
                                import base64
                                attachments.append({
                                    'filename': f.filename,
                                    'content_type': f.content_type or 'application/octet-stream',
                                    'data': base64.b64encode(file_bytes).decode('utf-8'),
                                })
            
            # Common document refs from form: common_document_0, common_document_name_0, etc.
            import re
            common_refs = []
            for form_key in request.form.keys():
                m = re.match(r'^common_document_(\d+)$', form_key)
                if m:
                    doc_id = request.form.get(form_key, '').strip()
                    name_key = f"common_document_name_{m.group(1)}"
                    doc_name = request.form.get(name_key, 'Common Document').strip()
                    if doc_id:
                        common_refs.append((doc_id, doc_name))
            for doc_id, doc_name in common_refs:
                attachments.append({
                    'type': 'common-document',
                    'ref': doc_id,
                    'document_id': doc_id,
                    'name': doc_name or 'Common Document',
                })
        else:
            data = request.get_json() or {}
            message = data.get('message', data.get('response_text', data.get('response', '')))
            send_email = data.get('sendEmail', False)
            attachments = data.get('attachments', [])
        
        if not message:
            return jsonify({'success': False, 'message': 'Message is required'}), 400
        
        # Get current member info
        current_member = safe_member_lookup()
        sender_name = current_member.get('name', 'Support Team') if current_member else 'Support Team'
        
        # Create reply record
        reply_data = {
            'ticket_id': ticket_id,
            'message': message,
            'sender_name': sender_name,
            'sender_id': session.get('member_id'),
            'sender_type': 'agent',
            'attachments': attachments,
            'created_at': datetime.now()
        }
        
        reply_id = db.create_reply(reply_data)
        
        # Emit real-time notification for new reply
        try:
            emit_new_reply(ticket_id, {
                'reply_id': str(reply_id),
                'ticket_id': ticket_id,
                'message': message,
                'sender_name': sender_name,
                'sender_type': 'agent',
                'attachments': len(attachments),
                'created_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit new reply event: {e}")
        
        # Update ticket with last reply info and clear draft
        db.update_ticket(ticket_id, {
            'last_reply_at': datetime.now(),
            'last_reply_by': sender_name,
            'updated_at': datetime.now(),
            'draft_body': ''  # Clear draft after sending reply
        })
        
        logger.info(f"Reply sent for ticket {ticket_id} by {sender_name}")
        
        # Always send reply via N8N webhook to Outlook when there's a customer email
        email_sent = False
        if ticket.get('email'):
            try:
                import requests
                from config.settings import WEBHOOK_URL
                logger.info(f"Preparing to send reply via N8N webhook to {ticket.get('email')}")
                
                # Prepare webhook payload matching N8N workflow expectations.
                # Include portal_reply_id so if n8n calls back /api/webhook/reply we skip duplicate (idempotency).
                
                # 🚀 RESOLVE attachment file data for webhook
                # Reply attachments may be stored on disk (file_path) without inline base64 data.
                import base64 as b64
                import os
                resolved_reply_attachments = []
                for att in attachments:
                    filename = att.get('filename', att.get('fileName', att.get('name', 'file')))
                    file_data = att.get('data', att.get('fileData', ''))
                    mime_type = att.get('content_type', 'application/octet-stream')
                    
                    # Strip data URI prefix if present (n8n expects raw base64)
                    if file_data and isinstance(file_data, str) and file_data.startswith('data:'):
                        # Remove "data:mime/type;base64," prefix
                        comma_idx = file_data.find(',')
                        if comma_idx > -1:
                            file_data = file_data[comma_idx + 1:]
                    
                    # If no inline data, try reading from disk via file_path
                    if not file_data or len(str(file_data)) < 10:
                        fp = att.get('file_path', att.get('path', ''))
                        if fp and os.path.exists(fp):
                            try:
                                with open(fp, 'rb') as f:
                                    fbytes = f.read()
                                file_data = b64.b64encode(fbytes).decode('utf-8')
                                logger.info(f"Reply attachment resolved from disk: {filename} ({len(fbytes)} bytes)")
                            except Exception as re:
                                logger.error(f"Failed to read reply attachment {fp}: {re}")
                    
                    resolved_reply_attachments.append({
                        'filename': filename,
                        'data': file_data,
                        'content_type': mime_type,
                        'size': att.get('size', 0)
                    })
                
                # Handle custom @VHC_Link tag replacement for emails
                import re
                
                def _strip_html(text):
                    """Strip any HTML tags from text, keeping content."""
                    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
                    text = re.sub(r'<a\s+[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', r'\2 \1', text, flags=re.IGNORECASE)
                    text = re.sub(r'<[^>]+>', '', text)
                    return text
                
                ticket_vhc_link = ticket.get('vhc_link', '').strip()
                
                # Build PLAIN TEXT directly from original message (no HTML roundtrip)
                message_plain = _strip_html(message)  # Strip any pre-existing HTML
                if ticket_vhc_link:
                    message_plain = re.sub(r'(@VHC_Link|\[VHC_LINK\])', f'Vehicle Health Check: {ticket_vhc_link}', message_plain, flags=re.IGNORECASE)
                
                # Build HTML version (reference only - n8n sends plain text)
                html_message = message.replace('\n', '<br>\n')
                if ticket_vhc_link:
                    html_link = f'<a href="{ticket_vhc_link}" target="_blank" style="color: #4f46e5; font-weight: 500; text-decoration: underline;">Vehicle Health Check — click here</a>'
                    html_message = re.sub(r'(@VHC_Link|\[VHC_LINK\])', html_link, html_message, flags=re.IGNORECASE)
                
                webhook_payload = {
                    'ticket_id': ticket_id,
                    'portal_reply_id': str(reply_id),
                    'response_text': message_plain,
                    'replyMessage': message_plain,
                    'html_message': html_message,
                    
                    'customer_email': ticket.get('email'),
                    'email': ticket.get('email'),
                    'ticket_subject': ticket.get('subject', 'Your Support Request'),
                    'subject': ticket.get('subject', 'Your Support Request'),
                    'customer_name': ticket.get('customer_name', ticket.get('name', '')),
                    'priority': ticket.get('priority', 'Medium'),
                    'ticket_status': ticket.get('status', 'Waiting for Response'),
                    'ticketSource': ticket.get('source', 'manual'),
                    'is_email_ticket': ticket.get('is_email_ticket', False),
                    'threadId': ticket.get('threadId', ''),
                    'message_id': ticket.get('message_id', ''),
                    'timestamp': datetime.now().isoformat(),
                    'user_id': session.get('member_id'),
                    'has_attachments': len(resolved_reply_attachments) > 0,
                    'attachments': resolved_reply_attachments,
                    'attachment_count': len(resolved_reply_attachments),
                    'body': ticket.get('body', ''),
                    'draft': message,
                    'message': message_plain,
                    'content': message_plain
                }
                
                logger.info(f"Sending reply to N8N webhook for ticket {ticket_id}")
                
                webhook_response = requests.post(
                    WEBHOOK_URL,
                    json=webhook_payload,
                    timeout=30
                )
                
                email_sent = webhook_response.status_code == 200
                logger.info(f"N8N webhook response for ticket {ticket_id}: {webhook_response.status_code}")
                
            except requests.exceptions.Timeout:
                logger.error(f"N8N webhook timeout for ticket {ticket_id}")
            except Exception as email_error:
                logger.error(f"Failed to send via N8N webhook for ticket {ticket_id}: {email_error}")
        
        return jsonify({
            'success': True,
            'message': 'Reply sent successfully',
            'reply_id': str(reply_id),
            'ticket_id': ticket_id,
            'email_sent': email_sent
        })
        
    except Exception as e:
        logger.error(f"Error sending reply for ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@ticket_bp.route('/search', methods=['GET'])
def search_tickets():
    """
    Search tickets with filters.
    
    Query params:
        q: Search query
        status: Status filter
        priority: Priority filter
        classification: Classification filter
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        query = request.args.get('q', '')
        status = request.args.get('status')
        priority = request.args.get('priority')
        classification = request.args.get('classification')
        
        from database import get_db
        db = get_db()
        
        tickets = db.search_tickets(
            query=query,
            status=status,
            priority=priority,
            classification=classification
        )
        
        serialized_tickets = [_serialize_ticket(t) for t in tickets]
        
        return jsonify({
            'success': True,
            'tickets': serialized_tickets,
            'count': len(serialized_tickets)
        })
        
    except Exception as e:
        logger.error(f"Error searching tickets: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/send-email', methods=['POST'])
def send_ticket_email(ticket_id):
    """
    Send an email from a template (or custom).
    Similar to reply, but allows custom subject and body.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from database import get_db
        db = get_db()
        
        # Get ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
            
        data = request.get_json()
        subject = data.get('custom_subject') or data.get('subject') or ticket.get('subject')
        body = data.get('custom_body') or data.get('body') or data.get('message')
        attachments = data.get('attachments', [])
        
        # 🔍 DEBUG: Log exactly what the frontend sent
        logger.info(f"[SEND-EMAIL DEBUG] Ticket: {ticket_id}")
        logger.info(f"[SEND-EMAIL DEBUG] Raw attachments from frontend: {len(attachments)} items")
        for i, att in enumerate(attachments):
            att_name = att.get('name', att.get('filename', 'NO_NAME'))
            att_data_len = len(str(att.get('fileData', att.get('data', '')))) if att.get('fileData') or att.get('data') else 0
            att_file_path = att.get('file_path', att.get('path', 'NO_PATH'))
            att_doc_id = att.get('document_id', 'NO_DOC_ID')
            att_ticket_idx = att.get('ticket_index', 'NO_IDX')
            logger.info(f"[SEND-EMAIL DEBUG] Attachment {i}: name={att_name}, data_len={att_data_len}, file_path={att_file_path}, doc_id={att_doc_id}, ticket_index={att_ticket_idx}")
        
        # Also log ticket's own stored attachments for comparison
        ticket_atts = ticket.get('attachments', [])
        logger.info(f"[SEND-EMAIL DEBUG] Ticket's own stored attachments: {len(ticket_atts)} items")
        for i, ta in enumerate(ticket_atts):
            ta_name = ta.get('filename', ta.get('name', ta.get('fileName', 'NO_NAME')))
            ta_has_data = bool(ta.get('data') or ta.get('fileData'))
            ta_file_path = ta.get('file_path', 'NO_FILE_PATH')
            logger.info(f"[SEND-EMAIL DEBUG] Ticket att {i}: name={ta_name}, has_data={ta_has_data}, file_path={ta_file_path}")
        
        # 🚀 FALLBACK: If frontend sent 0 attachments but ticket has stored attachments, use ticket's own
        if len(attachments) == 0 and len(ticket_atts) > 0:
            logger.info(f"[SEND-EMAIL FALLBACK] Frontend sent 0 attachments, using ticket's {len(ticket_atts)} stored attachments")
            attachments = ticket_atts
        
        if not body:
            return jsonify({'success': False, 'error': 'Email body is required'}), 400

        # Get current member info
        current_member = safe_member_lookup()
        sender_name = current_member.get('name', 'Support Team') if current_member else 'Support Team'
        
        # Create reply record (so it shows in history)
        reply_data = {
            'ticket_id': ticket_id,
            'message': body, # Store the body as the message
            'subject': subject, # Store subject if schema supports it, or just in body
            'sender_name': sender_name,
            'sender_id': session.get('member_id'),
            'sender_type': 'agent',
            'attachments': attachments,
            'created_at': datetime.now(),
            'is_email_template': True # detailed flag
        }
        
        reply_id = db.create_reply(reply_data)
        
        # Update ticket
        db.update_ticket(ticket_id, {
            'last_reply_at': datetime.now(),
            'last_reply_by': sender_name,
            'updated_at': datetime.now()
        })
        
        logger.info(f"Email template sent for ticket {ticket_id} by {sender_name}")
        
        # Send via N8N webhook
        email_sent = False
        if ticket.get('email'):
            try:
                import requests
                import base64
                import os
                from config.settings import WEBHOOK_URL
                
                # 🚀 RESOLVE attachment file data for webhook
                # Manual ticket attachments are stored on disk (file_path) without inline data.
                # We must read from disk and base64-encode so n8n receives actual file content.
                # NOTE: The frontend may send web URLs as file_path (not disk paths), so we
                # always fall back to matching against the ticket's stored attachments by filename.
                ticket_atts = ticket.get('attachments', [])
                resolved_attachments = []
                for att in attachments:
                    filename = att.get('name', att.get('filename', att.get('fileName', 'file')))
                    file_data = att.get('fileData', att.get('data', ''))
                    
                    logger.info(f"[EMAIL-ATT] Processing: {filename}, inline_data_len={len(str(file_data)) if file_data else 0}")
                    
                    # If no inline data, try reading from disk via file_path
                    if not file_data or len(str(file_data)) < 10:
                        file_path = att.get('file_path', att.get('path', ''))
                        logger.info(f"[EMAIL-ATT] No inline data. file_path from frontend: '{file_path}'")
                        if file_path and os.path.exists(file_path):
                            try:
                                with open(file_path, 'rb') as f:
                                    file_bytes = f.read()
                                file_data = base64.b64encode(file_bytes).decode('utf-8')
                                logger.info(f"[EMAIL-ATT] ✅ Read from disk path: {filename} ({len(file_bytes)} bytes)")
                            except Exception as read_err:
                                logger.error(f"[EMAIL-ATT] ❌ Failed to read from disk {file_path}: {read_err}")
                        else:
                            logger.info(f"[EMAIL-ATT] file_path not a valid disk path (may be a URL)")
                    
                    # If still no data, try common document lookup
                    if (not file_data or len(str(file_data)) < 10) and att.get('document_id'):
                        try:
                            doc = db.get_common_document(att['document_id'])
                            if doc:
                                doc_data = doc.get('data') or doc.get('fileData') or doc.get('content') or doc.get('file_data') or ''
                                if doc_data:
                                    file_data = doc_data
                                    logger.info(f"[EMAIL-ATT] ✅ Resolved from common document: {filename}")
                                elif doc.get('file_path') and os.path.exists(doc.get('file_path')):
                                    with open(doc['file_path'], 'rb') as f:
                                        file_bytes = f.read()
                                    file_data = base64.b64encode(file_bytes).decode('utf-8')
                                    logger.info(f"[EMAIL-ATT] ✅ Read common doc from disk: {filename} ({len(file_bytes)} bytes)")
                        except Exception as doc_err:
                            logger.error(f"[EMAIL-ATT] ❌ Failed common document lookup {att.get('document_id')}: {doc_err}")
                    
                    # If still no data, try ticket_index (with safe parsing)
                    if (not file_data or len(str(file_data)) < 10):
                        raw_idx = att.get('ticket_index')
                        if raw_idx is not None and str(raw_idx).strip() != '':
                            try:
                                idx = int(raw_idx)
                                if 0 <= idx < len(ticket_atts):
                                    source_att = ticket_atts[idx]
                                    stored_data = source_att.get('data') or source_att.get('fileData') or ''
                                    if stored_data and len(str(stored_data)) >= 10:
                                        file_data = stored_data
                                        logger.info(f"[EMAIL-ATT] ✅ Resolved from ticket index {idx}: {filename}")
                                    elif source_att.get('file_path') and os.path.exists(source_att['file_path']):
                                        with open(source_att['file_path'], 'rb') as f:
                                            file_bytes = f.read()
                                        file_data = base64.b64encode(file_bytes).decode('utf-8')
                                        logger.info(f"[EMAIL-ATT] ✅ Read ticket att from disk index {idx}: {filename} ({len(file_bytes)} bytes)")
                            except (ValueError, TypeError) as idx_err:
                                logger.warning(f"[EMAIL-ATT] ticket_index parse failed ('{raw_idx}'): {idx_err}")
                    
                    # 🔥 FINAL FALLBACK: Match against ticket's stored attachments by filename
                    # This handles cases where frontend sends a web URL as file_path
                    if not file_data or len(str(file_data)) < 10:
                        logger.info(f"[EMAIL-ATT] All methods failed, trying filename match against {len(ticket_atts)} ticket attachments")
                        for stored_att in ticket_atts:
                            stored_name = stored_att.get('filename', stored_att.get('name', stored_att.get('fileName', '')))
                            if stored_name == filename:
                                # Try inline data first
                                stored_data = stored_att.get('data') or stored_att.get('fileData') or ''
                                if stored_data and len(str(stored_data)) >= 10:
                                    file_data = stored_data
                                    logger.info(f"[EMAIL-ATT] ✅ Matched by filename, got inline data: {filename}")
                                    break
                                # Try disk path
                                stored_fp = stored_att.get('file_path', '')
                                if stored_fp and os.path.exists(stored_fp):
                                    try:
                                        with open(stored_fp, 'rb') as f:
                                            file_bytes = f.read()
                                        file_data = base64.b64encode(file_bytes).decode('utf-8')
                                        logger.info(f"[EMAIL-ATT] ✅ Matched by filename, read from disk: {filename} ({len(file_bytes)} bytes)")
                                        break
                                    except Exception as e:
                                        logger.error(f"[EMAIL-ATT] ❌ Filename match disk read failed: {e}")
                    
                    data_len = len(str(file_data)) if file_data else 0
                    mime_type = att.get('content_type', 'application/octet-stream')
                    
                    if data_len < 10:
                        logger.warning(f"[EMAIL-ATT] ⚠️ UNRESOLVED attachment: {filename} (data_len={data_len})")
                    
                    # Strip data URI prefix if present (n8n expects raw base64)
                    if file_data and isinstance(file_data, str) and file_data.startswith('data:'):
                        comma_idx = file_data.find(',')
                        if comma_idx > -1:
                            file_data = file_data[comma_idx + 1:]
                    
                    resolved_attachments.append({
                        'filename': filename,
                        'data': file_data,
                        'content_type': mime_type
                    })
                    logger.info(f"[EMAIL-ATT] Final: {filename}, data_length={data_len}")
                
                # Convert and handle HTML/VHC for email
                import re as _re
                
                def _strip_html(html_str):
                    """Strip HTML to plain text."""
                    text = _re.sub(r'<br\s*/?>', '\n', html_str, flags=_re.IGNORECASE)
                    text = _re.sub(r'<a\s+[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', r'\2 (\1)', text, flags=_re.IGNORECASE)
                    text = _re.sub(r'<[^>]+>', '', text)
                    text = _re.sub(r'\n{3,}', '\n\n', text)
                    return text.strip()
                
                ticket_vhc_link = ticket.get('vhc_link', '').strip()
                
                # Build PLAIN TEXT directly from body (no HTML roundtrip)
                body_plain = body
                if ticket_vhc_link:
                    body_plain = _re.sub(r'(@VHC_Link|\[VHC_LINK\])', f'Vehicle Health Check: {ticket_vhc_link}', body_plain, flags=_re.IGNORECASE)
                # Strip any pre-existing HTML tags
                body_plain = _re.sub(r'<br\s*/?>', '\n', body_plain, flags=_re.IGNORECASE)
                body_plain = _re.sub(r'<a\s+[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', r'\2 \1', body_plain, flags=_re.IGNORECASE)
                body_plain = _re.sub(r'<[^>]+>', '', body_plain)
                
                # Build HTML version (reference only - n8n sends plain text)
                html_body = body.replace('\n', '<br>\n')
                if ticket_vhc_link:
                    html_link = f'<a href="{ticket_vhc_link}" target="_blank" style="color: #4f46e5; font-weight: 500; text-decoration: underline;">Vehicle Health Check — click here</a>'
                    html_body = _re.sub(r'(@VHC_Link|\[VHC_LINK\])', html_link, html_body, flags=_re.IGNORECASE)
                
                # Payload with OVERRIDDEN subject and body
                webhook_payload = {
                    'ticket_id': ticket_id,
                    'response_text': body_plain,
                    'replyMessage': body_plain,
                    'html_message': html_body,
                    'customer_email': ticket.get('email'),
                    'email': ticket.get('email'),
                    'ticket_subject': subject,
                    'subject': subject,
                    'customer_name': ticket.get('customer_name', ticket.get('name', '')),
                    'priority': ticket.get('priority', 'Medium'),
                    'ticket_status': ticket.get('status', 'Waiting for Response'),
                    'ticketSource': 'email template',
                    'source': 'email template',
                    'user_id': session.get('member_id'),
                    'has_attachments': len(resolved_attachments) > 0,
                    'attachments': resolved_attachments,
                    'attachment_count': len(resolved_attachments),
                    'body': ticket.get('body', ''), 
                    'message': body_plain,
                    'content': body_plain
                }
                
                logger.info(f"Sending email template to N8N webhook for ticket {ticket_id}")
                
                webhook_response = requests.post(
                    WEBHOOK_URL,
                    json=webhook_payload,
                    timeout=30
                )
                
                email_sent = webhook_response.status_code == 200
                logger.info(f"N8N webhook response: {webhook_response.status_code}")
                
            except Exception as email_error:
                logger.error(f"Failed to send email template via N8N: {email_error}")
        
        if not email_sent:
             return jsonify({
                'success': True, # Still success because we saved the reply? Or Warning?
                'warning': 'Response saved but email delivery failed (Webhook Error)',
                'email_sent': False,
                'reply_id': str(reply_id)
            })

        return jsonify({
            'success': True,
            'message': 'Email sent successfully',
            'reply_id': str(reply_id),
            'email_sent': True
        })
        
    except Exception as e:
        logger.error(f"Error sending email template {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def _serialize_ticket(ticket):
    """
    Serialize a ticket document for JSON response.
    Handles ObjectId and datetime conversions.
    """
    if not ticket:
        return None
    
    serialized = {}
    for key, value in ticket.items():
        if key == '_id':
            serialized['_id'] = str(value)
        elif isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, list):
            serialized[key] = [
                _serialize_ticket(item) if isinstance(item, dict) else 
                str(item) if hasattr(item, '__str__') and not isinstance(item, (str, int, float, bool)) else item
                for item in value
            ]
        elif isinstance(value, dict):
            serialized[key] = _serialize_ticket(value)
        else:
            serialized[key] = value
    
    return serialized

@ticket_bp.route('/<ticket_id>/priority', methods=['POST'])
def update_ticket_priority(ticket_id):
    """Update ticket priority."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        data = request.get_json()
        priority = data.get('priority')
        
        if not priority:
            return jsonify({'success': False, 'error': 'Priority is required'}), 400
            
        from database import get_db
        db = get_db()
        
        # Update priority
        update_data = {
            'priority': priority,
            'updated_at': datetime.now()
        }
        
        # Get current priority before update
        ticket = db.get_ticket_by_id(ticket_id)
        old_priority = ticket.get('priority') if ticket else None
        
        db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} priority updated to {priority} by {session.get('member_name')}")
        
        # Emit real-time WebSocket event for priority change
        # Emit real-time WebSocket event for priority change
        try:
            emit_priority_changed(ticket_id, {
                'ticket_id': ticket_id,
                'old_priority': old_priority,
                'new_priority': priority,
                'changed_by_id': session.get('member_id'),
                'changed_by_name': session.get('member_name'),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit priority change event: {e}")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': f'Priority updated to {priority}'
        })
        
    except Exception as e:
        logger.error(f"Error updating ticket priority {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/technician', methods=['POST'])
def update_ticket_technician(ticket_id):
    """Update assigned technician."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
            
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
            
        data = request.get_json()
        technician_id = data.get('technician_id')
        
        from database import get_db
        db = get_db()
        
        update_data = {
            'assigned_technician_id': technician_id,
            'technician_id': technician_id, # Keep both for compatibility
            'updated_at': datetime.now()
        }
        
        # If unassigning
        if not technician_id:
             update_data['status'] = 'New' # Revert to New or Open?
             update_data['assigned_technician'] = None
             msg = 'Technician unassigned'
        else:
            # Get technician name for history/display
            tech = db.get_technician_by_id(technician_id)
            if tech:
                update_data['assigned_technician'] = tech.get('name')
            update_data['status'] = 'Assigned'
            msg = f"Technician assigned"
            
        db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} technician updated to {technician_id} by {session.get('member_name')}")
        
        # Emit real-time WebSocket event for technician assignment
        # Emit real-time WebSocket event for technician assignment
        try:
            ticket = db.get_ticket_by_id(ticket_id)
            emit_technician_assigned(ticket_id, {
                'ticket_id': ticket_id,
                'subject': ticket.get('subject', '') if ticket else '',
                'technician_id': technician_id,
                'technician_name': update_data.get('assigned_technician', ''),
                'assigned_by_id': session.get('member_id'),
                'assigned_by_name': session.get('member_name'),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit technician assignment event: {e}")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': msg
        })
        
    except Exception as e:
        logger.error(f"Error updating ticket technician {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/assign', methods=['POST'])
def assign_ticket(ticket_id):
    """
    Assign ticket (Take Over or Forward).
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
            
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
            
        data = request.get_json() or {}
        is_forwarded = data.get('is_forwarded', False)
        target_member_id = data.get('assigned_to')
        note = data.get('note', '')
        
        from database import get_db
        db = get_db()
        
        current_member_id = session.get('member_id')
        current_member_name = session.get('member_name')
        
        # Fetch ticket to get old status (needed for event emission)
        ticket_before = db.get_ticket_by_id(ticket_id)
        if not ticket_before:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
            
        old_status = ticket_before.get('status')
        
        update_data = {
            'updated_at': datetime.now()
        }
        
        if is_forwarded:
            # Forwarding to another member
            if not target_member_id:
                return jsonify({'success': False, 'error': 'Target member required for forwarding'}), 400
            
            # Convert target_member_id to ObjectId if it's a string
            from bson.objectid import ObjectId
            try:
                if isinstance(target_member_id, str):
                    target_member_id = ObjectId(target_member_id)
            except:
                pass  # If conversion fails, use as-is
            
            # Convert current_member_id to ObjectId for proper member lookup
            try:
                current_member_id_obj = ObjectId(current_member_id) if isinstance(current_member_id, str) else current_member_id
            except:
                current_member_id_obj = current_member_id
                
            update_data['is_forwarded'] = True
            update_data['forwarded_by'] = current_member_id_obj  # Store as ObjectId for proper member lookup
            update_data['forwarded_to'] = target_member_id
            update_data['forwarded_at'] = datetime.now()
            update_data['forwarding_note'] = note
            update_data['status'] = 'Open' # Default status for forwarded tickets
            
            # Create assignment record for forwarded ticket
            assignment_data = {
                'ticket_id': ticket_id,
                'member_id': target_member_id,  # The person it's forwarded TO
                'forwarded_from': current_member_id_obj,  # The person forwarding it (as ObjectId)
                'is_forwarded': True,
                'assigned_at': datetime.now(),
                'notes': note,
                'is_seen': False  # Forwarded tickets start as unseen
            }
            db.assign_ticket(assignment_data)
            
            msg = 'Ticket forwarded successfully'
            
        else:
            # Take Over (Assign to self)
            update_data['assigned_to'] = current_member_id
            update_data['assigned_by'] = current_member_id
            update_data['assigned_at'] = datetime.now()
            update_data['status'] = 'In Progress'
            
            # Create assignment record for takeover
            from bson.objectid import ObjectId
            try:
                current_member_obj_id = ObjectId(current_member_id) if isinstance(current_member_id, str) else current_member_id
            except:
                current_member_obj_id = current_member_id
                
            assignment_data = {
                'ticket_id': ticket_id,
                'member_id': current_member_obj_id,
                'is_forwarded': False,
                'assigned_at': datetime.now(),
                'is_seen': True  # Takeover assignments are seen immediately
            }
            db.assign_ticket(assignment_data)
            
            msg = 'Ticket taken over successfully'
        
        db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} assignment updated by {current_member_name}")
        
        # Emit real-time WebSocket events
        try:
            # Check for Status Change
            new_status = update_data.get('status')
            if new_status and new_status != old_status:
                emit_status_changed(ticket_id, {
                    'ticket_id': ticket_id,
                    'old_status': old_status,
                    'new_status': new_status,
                    'changed_by_id': str(current_member_id),
                    'changed_by_name': current_member_name,
                    'timestamp': datetime.now().isoformat()
                })

            ticket_subject = ticket_before.get('subject', '')
            
            if is_forwarded:
                # Get target member name
                target_member = db.get_member_by_id(target_member_id)
                target_name = target_member.get('name', 'Unknown') if target_member else 'Unknown'
                
                emit_ticket_forwarded(ticket_id, {
                    'ticket_id': ticket_id,
                    'subject': ticket_subject,
                    'forwarded_from_id': str(current_member_id),
                    'forwarded_from_name': current_member_name,
                    'forwarded_to_id': str(target_member_id),
                    'forwarded_to_name': target_name,
                    'note': note,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                # Get previous assignee if any
                previous_assignee = ticket_before.get('assigned_to')
                
                emit_ticket_taken_over(ticket_id, {
                    'ticket_id': ticket_id,
                    'subject': ticket_subject,
                    'taken_by_id': str(current_member_id),
                    'taken_by_name': current_member_name,
                    'previous_assignee_id': str(previous_assignee) if previous_assignee else None,
                    'timestamp': datetime.now().isoformat()
                })
        except Exception as e:
            logger.warning(f"Failed to emit assignment event: {e}")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': msg
        })
        
    except Exception as e:
        logger.error(f"Error assigning ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/tech-director', methods=['POST'])
def refer_to_tech_director(ticket_id):
    """Refer ticket to Technical Director.
    
    CRITICAL: This must set BOTH the legacy 'referred_to_director' flag AND the
    new forwarding fields (is_forwarded, forwarded_to, forwarded_by) so that
    the Tech Director query (get_forwarded_tickets_to_user) can find these tickets.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
            
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
            
        from database import get_db
        from bson.objectid import ObjectId
        db = get_db()
        
        # Find the Tech Director member to get their ID
        tech_director = db.members.find_one({'role': 'Technical Director', 'is_active': {'$ne': False}})
        if not tech_director:
            logger.error(f"No active Technical Director found in database")
            return jsonify({'success': False, 'error': 'No Technical Director found'}), 404
        
        tech_director_id = tech_director.get('_id')
        current_member_id = session.get('member_id')
        
        # Convert current_member_id to ObjectId if needed
        try:
            if isinstance(current_member_id, str):
                current_member_id_obj = ObjectId(current_member_id)
            else:
                current_member_id_obj = current_member_id
        except:
            current_member_id_obj = current_member_id
        
        # Get referral note from request body
        data = request.get_json(silent=True) or {}
        referral_note = data.get('referral_note', '').strip()
        
        update_data = {
            # Legacy fields (for backward compatibility)
            'referred_to_director': True,
            'referred_at': datetime.now(),
            'referred_by': current_member_id,
            'status': 'Referred to Tech Director',
            
            # CRITICAL: Forwarding fields for Tech Director query
            # The get_forwarded_tickets_to_user() method filters by these fields
            'is_forwarded': True,
            'forwarded_to': tech_director_id,  # Tech Director's ObjectId
            'forwarded_by': current_member_id_obj,  # Person referring the ticket
            'forwarded_at': datetime.now(),
            'is_forwarded_viewed': False,  # Mark as unviewed initially
            'forwarding_note': referral_note,  # Note for Tech Director dashboard
            'referral_note': referral_note   # Also keep as referral_note for clarity
        }
        
        # Add to Private Notes for full history
        private_note = None
        if referral_note:
            private_note = {
                'title': 'Forwarded to Tech Director',
                'content': referral_note,
                'author': session.get('member_name') or 'Admin',
                'timestamp': datetime.now().isoformat()
            }
            
        if private_note:
            db.tickets.update_one(
                {'ticket_id': ticket_id},
                {'$set': update_data, '$push': {'private_notes': private_note}}
            )
        else:
            db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} referred to Tech Director (ID: {tech_director_id}) by {session.get('member_name')}")
        
        # Emit real-time WebSocket event for Tech Director referral
        # Emit real-time WebSocket event for Tech Director referral
        try:
            ticket = db.get_ticket_by_id(ticket_id)
            emit_tech_director_referral(ticket_id, {
                'ticket_id': ticket_id,
                'subject': ticket.get('subject', '') if ticket else '',
                'referred_by_id': str(current_member_id),
                'referred_by_name': session.get('member_name'),
                'tech_director_id': str(tech_director_id),
                'tech_director_name': tech_director.get('name', 'Technical Director'),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit tech director referral event: {e}")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': 'Referred to Technical Director'
        })
        
    except Exception as e:
        logger.error(f"Error referring ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/refer-back-to-admin', methods=['POST'])
def refer_back_to_admin(ticket_id):
    """Tech Director refers ticket back to Admin.
    
    Clears the forwarding fields and resets status so the ticket
    reappears in the Admin's normal ticket queue.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
            
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
            
        from database import get_db
        db = get_db()
        
        data = request.get_json(silent=True) or {}
        referral_note = data.get('referral_note', '').strip()
        
        current_member_id = session.get('member_id')
        current_member_name = session.get('member_name', 'Tech Director')
        
        update_data = {
            # Reset status back to Open
            'status': 'Open',
            'referred_to_director': False,
            
            # Clear forwarding fields so it leaves the TD's forwarded queue
            'is_forwarded': False,
            'forwarded_to': None,
            'forwarded_by': None,
            'forwarded_at': None,
            'is_forwarded_viewed': False,
            
            # Keep a record of who sent it back and why
            'referred_back_by': current_member_id,
            'referred_back_by_name': current_member_name,
            'referred_back_at': datetime.now(),
            'referred_back_note': referral_note,
            # 'forwarding_note': referral_note if referral_note else None, # Do NOT set forwarding_note here, it breaks dashboard
        }
        
        # Keep a permanent record in Private Notes
        private_note = None
        if referral_note:
            private_note = {
                'title': 'Returned to Admin',
                'content': referral_note,
                'author': current_member_name,
                'timestamp': datetime.now().isoformat()
            }

        if private_note:
            db.tickets.update_one(
                {'ticket_id': ticket_id},
                {'$set': update_data, '$push': {'private_notes': private_note}}
            )
        else:
            db.update_ticket(ticket_id, update_data)
        
        logger.info(f"Ticket {ticket_id} referred back to Admin by {current_member_name}")
        
        return jsonify({
            'success': True,
            'status': 'success',
            'message': 'Ticket referred back to Admin'
        })
        
    except Exception as e:
        logger.error(f"Error referring ticket {ticket_id} back to admin: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/clear-resolved', methods=['POST'])
def clear_resolved_tickets():
    """Clear resolved tickets from the Tech Director's dashboard.
    
    Sets a td_cleared flag on tickets so they don't appear in the resolved list.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from database import get_db
        db = get_db()
        
        data = request.get_json(silent=True) or {}
        ticket_ids = data.get('ticket_ids', [])
        
        if not ticket_ids:
            return jsonify({'success': False, 'error': 'No ticket IDs provided'}), 400
        
        for tid in ticket_ids:
            db.update_ticket(tid, {'td_cleared': True})
        
        logger.info(f"Cleared {len(ticket_ids)} resolved tickets from TD dashboard")
        
        return jsonify({'success': True, 'cleared': len(ticket_ids)})
    except Exception as e:
        logger.error(f"Error clearing resolved tickets: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/important', methods=['POST'])
def toggle_ticket_importance(ticket_id):
    """Toggle ticket importance (Starred)."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
            
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
            
        from database import get_db
        db = get_db()
        
        # Get current state
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
             return jsonify({'success': False, 'error': 'Ticket not found'}), 404
             
        current_importance = ticket.get('is_important', False)
        new_importance = not current_importance
        
        update_data = {
            'is_important': new_importance,
            'updated_at': datetime.now()
        }
        
        db.update_ticket(ticket_id, update_data)
        
        # Emit real-time WebSocket event for bookmark change
        try:
            emit_bookmark_changed(ticket_id, {
                'ticket_id': ticket_id,
                'is_important': new_importance,
                'changed_by_id': session.get('member_id'),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to emit bookmark change event: {e}")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': 'Importance updated',
            'is_important': new_importance
        })
        
    except Exception as e:
        logger.error(f"Error toggling importance for {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/reply-count', methods=['GET'])
def get_reply_count(ticket_id):
    """Get the count of replies for a ticket."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        from database import get_db
        db = get_db()
        
        # Get all replies for this ticket
        replies = db.get_replies_by_ticket(ticket_id)
        count = len(replies) if replies else 0
        
        return jsonify({
            'success': True,
            'count': count,
            'ticket_id': ticket_id
        })
        
    except Exception as e:
        logger.error(f"Error getting reply count for {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/replies', methods=['GET'])
def get_ticket_replies(ticket_id):
    """
    Get all replies for a ticket as JSON for AJAX refresh on the ticket detail page.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401

        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400

        from database import get_db
        db = get_db()

        replies = db.get_replies_by_ticket(ticket_id) or []

        serialized = []
        for reply in replies:
            item = {}
            for key, value in reply.items():
                if key == '_id':
                    item['_id'] = str(value)
                elif isinstance(value, datetime):
                    item[key] = value.isoformat()
                else:
                    item[key] = value
            serialized.append(item)

        return jsonify({
            'success': True,
            'ticket_id': ticket_id,
            'replies': serialized,
            'count': len(serialized)
        })

    except Exception as e:
        logger.error(f"Error getting replies for ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@ticket_bp.route('/<ticket_id>/outcome', methods=['POST'])
def update_ticket_outcome(ticket_id):
    """
    Update outcome assessment for a ticket.
    Persists outcome_category, outcome_notes, revisit_carried_out, clean_under_warranty to the ticket in MongoDB.
    """
    try:
        if not is_authenticated():
            return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401

        if not validate_ticket_id(ticket_id):
            return jsonify({'status': 'error', 'message': 'Invalid ticket ID'}), 400

        data = request.get_json(silent=True) or {}
        outcome_category = (data.get('outcome_category') or '').strip()
        revisit_carried_out = '1' if data.get('revisit_carried_out') else '0'
        clean_under_warranty = '1' if data.get('clean_under_warranty') else '0'
        outcome_notes = (data.get('outcome_notes') or '').strip()

        revisit_date = (data.get('revisit_date') or '').strip()
        revisit_technician_id = (data.get('revisit_technician_id') or '').strip()
        revisit_reason = (data.get('revisit_reason') or '').strip()

        from database import get_db
        db = get_db()

        ticket = db.tickets.find_one({'ticket_id': ticket_id})
        if not ticket:
            return jsonify({'status': 'error', 'message': 'Ticket not found'}), 404

        update_data = {
            'outcome_category': outcome_category,
            'outcome_notes': outcome_notes,
            'revisit_carried_out': revisit_carried_out,
            'clean_under_warranty': clean_under_warranty,
            'updated_at': datetime.now()
        }

        # Handle "Revisit" specific fields
        if outcome_category == 'Revisit':
            update_data['revisit_date'] = revisit_date
            update_data['revisit_technician_id'] = revisit_technician_id
            update_data['revisit_reason'] = revisit_reason
        else:
            # Clear them if the category was changed to something else
            update_data['revisit_date'] = ''
            update_data['revisit_technician_id'] = ''
            update_data['revisit_reason'] = ''
        db.tickets.update_one(
            {'ticket_id': ticket_id},
            {'$set': update_data}
        )

        logger.info(f"Outcome updated for ticket {ticket_id} by {session.get('member_name') or session.get('member_id')}")
        return jsonify({
            'status': 'success',
            'message': 'Outcome information updated successfully'
        })

    except Exception as e:
        logger.error(f"Error updating outcome for ticket {ticket_id}: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ==========================================
# PRIVATE NOTES ENDPOINTS
# ==========================================

@ticket_bp.route('/<ticket_id>/private-notes', methods=['GET'])
def get_private_notes(ticket_id):
    """Get all private notes for a ticket."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
            
        from database import get_db
        db = get_db()
        ticket = db.tickets.find_one({'ticket_id': ticket_id}, {'private_notes': 1})
        
        if not ticket:
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
            
        notes = ticket.get('private_notes', [])
        return jsonify({'success': True, 'notes': notes})
        
    except Exception as e:
        logger.error(f"Error getting private notes for ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ticket_bp.route('/<ticket_id>/private-notes', methods=['POST'])
def add_private_note(ticket_id):
    """Add a new private note to a ticket."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
            
        data = request.get_json()
        if not data or not data.get('title') or not data.get('content'):
            return jsonify({'success': False, 'error': 'Title and content are required'}), 400
            
        current_member_name = session.get('member_name') or session.get('user_id') or 'Unknown Admin'
            
        note = {
            'title': data['title'].strip(),
            'content': data['content'].strip(),
            'author': current_member_name,
            'timestamp': datetime.now().isoformat()
        }
        
        from database import get_db
        db = get_db()
        
        # Check if updating existing or adding new
        note_index = data.get('index')
        if note_index is not None and isinstance(note_index, int) and note_index >= 0:
            # Update existing note at index
            update_field = f'private_notes.{note_index}'
            result = db.tickets.update_one(
                {'ticket_id': ticket_id},
                {'$set': {update_field: note, 'updated_at': datetime.now()}}
            )
        else:
            # Append new note
            result = db.tickets.update_one(
                {'ticket_id': ticket_id},
                {
                    '$push': {'private_notes': note},
                    '$set': {'updated_at': datetime.now()}
                }
            )
            
        if result.modified_count == 0:
            # Maybe the ticket doesn't have a private_notes array yet, or index is out of bounds
            # If standard push failed, the ticket might not exist
            pass
            
        return jsonify({'success': True, 'message': 'Note saved', 'note': note})
        
    except Exception as e:
        logger.error(f"Error adding private note: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ticket_bp.route('/<ticket_id>/private-notes/<int:note_index>', methods=['DELETE'])
def delete_private_note(ticket_id, note_index):
    """Delete a private note by its index."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
            
        from database import get_db
        db = get_db()
        
        # MongoDB doesn't have a simple way to pull by index, so we:
        # 1. Unset the element at the index (sets it to null)
        # 2. Pull nulls from the array
        
        db.tickets.update_one(
            {'ticket_id': ticket_id},
            {'$unset': {f'private_notes.{note_index}': 1}}
        )
        
        db.tickets.update_one(
            {'ticket_id': ticket_id},
            {
                '$pull': {'private_notes': None},
                '$set': {'updated_at': datetime.now()}
            }
        )
        
        return jsonify({'success': True, 'message': 'Note deleted'})
        
    except Exception as e:
        logger.error(f"Error deleting private note: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/mark-forwarded-viewed', methods=['POST'])
def mark_forwarded_viewed(ticket_id):
    """Mark a forwarded ticket as viewed by the current user."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from utils.validators import validate_ticket_id
        if not validate_ticket_id(ticket_id):
            return jsonify({'success': False, 'error': 'Invalid ticket ID'}), 400
        
        current_member_id = session.get('member_id')
        if not current_member_id:
            return jsonify({'success': False, 'error': 'User not found in session'}), 401
            
        from database import get_db
        db = get_db()
        
        success = db.mark_forwarded_ticket_viewed(ticket_id, current_member_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Marked as viewed',
                'ticket_id': ticket_id
            })
        else:
            return jsonify({
                'success': False, 
                'error': 'Failed to mark as viewed or ticket not forwarded to you'
            }), 400
            
    except Exception as e:
        logger.error(f"Error marking ticket {ticket_id} as viewed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/attachments/<int:attachment_index>/download', methods=['GET'])
def download_ticket_attachment(ticket_id, attachment_index):
    """
    Download attachment from a ticket.
    
    Handles multiple attachment storage formats:
    1. Base64 encoded data in 'data' or 'fileData' field
    2. File path reference in 'file_path' field
    3. Common document reference via 'document_id' field
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from database import get_db
        from flask import make_response
        import base64
        import os
        db = get_db()
        
        # Get ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            logger.error(f"[DOWNLOAD] Ticket not found: {ticket_id}")
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        # Get attachments array
        attachments = ticket.get('attachments', [])
        
        if attachment_index < 0 or attachment_index >= len(attachments):
            logger.error(f"[DOWNLOAD] Attachment index {attachment_index} out of range (0-{len(attachments)-1}) for ticket {ticket_id}")
            return jsonify({'success': False, 'error': 'Attachment not found'}), 404
        
        attachment = attachments[attachment_index]
        logger.info(f"[DOWNLOAD] Processing attachment {attachment_index} for ticket {ticket_id}: {attachment}")
        
        # Get filename
        filename = attachment.get('filename', attachment.get('fileName', attachment.get('name', 'download')))
        
        # Try multiple sources for file data
        file_data = None
        
        # 1. Try base64 encoded data (most common for email attachments)
        if attachment.get('data') or attachment.get('fileData'):
            base64_data = attachment.get('data') or attachment.get('fileData')
            try:
                file_data = base64.b64decode(base64_data)
                logger.info(f"[DOWNLOAD] Decoded {len(file_data)} bytes from base64 for {filename}")
            except Exception as e:
                logger.error(f"[DOWNLOAD] Failed to decode base64 data: {e}")
        
        # 2. Try file path
        if not file_data and attachment.get('file_path'):
            file_path = attachment.get('file_path')
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                logger.info(f"[DOWNLOAD] Read {len(file_data)} bytes from file path for {filename}")
            else:
                logger.warning(f"[DOWNLOAD] File path does not exist: {file_path}")
        
        # 3. Try common document reference
        if not file_data and attachment.get('document_id'):
            doc_id = attachment.get('document_id')
            try:
                from bson.objectid import ObjectId
                doc = db.common_documents.find_one({'_id': ObjectId(doc_id)})
                if doc and doc.get('file_path') and os.path.exists(doc.get('file_path')):
                    with open(doc.get('file_path'), 'rb') as f:
                        file_data = f.read()
                    logger.info(f"[DOWNLOAD] Read {len(file_data)} bytes from common document for {filename}")
            except Exception as e:
                logger.warning(f"[DOWNLOAD] Could not load common document {doc_id}: {e}")
        
        if not file_data:
            logger.error(f"[DOWNLOAD] No file data available for attachment {attachment_index} in ticket {ticket_id}")
            return jsonify({'success': False, 'error': 'Attachment data not available'}), 404
        
        # Determine MIME type
        from utils.file_utils import get_mime_type
        mime_type = get_mime_type(filename)
        
        # Create response with proper headers
        response = make_response(file_data)
        response.headers['Content-Type'] = mime_type
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        response.headers['Content-Length'] = len(file_data)
        
        logger.info(f"[DOWNLOAD] Successfully serving {filename} ({len(file_data)} bytes, {mime_type})")
        return response
        
    except Exception as e:
        logger.error(f"[DOWNLOAD] Error downloading attachment for ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/attachments/<int:attachment_index>/preview', methods=['GET'])
def preview_ticket_attachment(ticket_id, attachment_index):
    """
    Preview attachment from a ticket inline (for images, PDFs, etc.).
    
    Same logic as download but uses Content-Disposition: inline for browser display.
    """
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        from database import get_db
        from flask import make_response
        import base64
        import os
        db = get_db()
        
        # Get ticket
        ticket = db.get_ticket_by_id(ticket_id)
        if not ticket:
            logger.error(f"[PREVIEW] Ticket not found: {ticket_id}")
            return jsonify({'success': False, 'error': 'Ticket not found'}), 404
        
        # Get attachments array
        attachments = ticket.get('attachments', [])
        
        if attachment_index < 0 or attachment_index >= len(attachments):
            logger.error(f"[PREVIEW] Attachment index {attachment_index} out of range for ticket {ticket_id}")
            return jsonify({'success': False, 'error': 'Attachment not found'}), 404
        
        attachment = attachments[attachment_index]
        logger.info(f"[PREVIEW] Processing attachment {attachment_index} for ticket {ticket_id}")
        
        # Get filename
        filename = attachment.get('filename', attachment.get('fileName', attachment.get('name', 'preview')))
        
        # Try multiple sources for file data
        file_data = None
        
        # 1. Try base64 encoded data
        if attachment.get('data') or attachment.get('fileData'):
            base64_data = attachment.get('data') or attachment.get('fileData')
            try:
                file_data = base64.b64decode(base64_data)
                logger.info(f"[PREVIEW] Decoded {len(file_data)} bytes from base64 for {filename}")
            except Exception as e:
                logger.error(f"[PREVIEW] Failed to decode base64 data: {e}")
        
        # 2. Try file path
        if not file_data and attachment.get('file_path'):
            file_path = attachment.get('file_path')
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                logger.info(f"[PREVIEW] Read {len(file_data)} bytes from file path for {filename}")
        
        # 3. Try common document reference
        if not file_data and attachment.get('document_id'):
            doc_id = attachment.get('document_id')
            try:
                from bson.objectid import ObjectId
                doc = db.common_documents.find_one({'_id': ObjectId(doc_id)})
                if doc and doc.get('file_path') and os.path.exists(doc.get('file_path')):
                    with open(doc.get('file_path'), 'rb') as f:
                        file_data = f.read()
                    logger.info(f"[PREVIEW] Read {len(file_data)} bytes from common document for {filename}")
            except Exception as e:
                logger.warning(f"[PREVIEW] Could not load common document {doc_id}: {e}")
        
        if not file_data:
            logger.error(f"[PREVIEW] No file data available for attachment {attachment_index} in ticket {ticket_id}")
            return jsonify({'success': False, 'error': 'Attachment data not available'}), 404
        
        # Determine MIME type
        from utils.file_utils import get_mime_type
        mime_type = get_mime_type(filename)
        
        # Create response for inline display
        response = make_response(file_data)
        response.headers['Content-Type'] = mime_type
        response.headers['Content-Disposition'] = f'inline; filename="{filename}"'
        response.headers['Content-Length'] = len(file_data)
        
        logger.info(f"[PREVIEW] Successfully serving preview of {filename} ({len(file_data)} bytes, {mime_type})")
        return response
        
    except Exception as e:
        logger.error(f"[PREVIEW] Error previewing attachment for ticket {ticket_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
