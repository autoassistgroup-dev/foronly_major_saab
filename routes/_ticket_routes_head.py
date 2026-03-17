#!/usr/bin/e python3
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
        
        from database import get_db
        db = get_db()
        
        # Get pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        status_filter = request.args.get('status')
        priority_filter = request.args.get('priority')
        search_query = request.args.get('search', '')
        
        # Get tickets with pagination
        tickets = db.get_tickets_with_assignments(
            page=page,
            per_page=per_page,
            status_filter=status_filter,
            priority_filter=priority_filter,
            search_query=search_query
        )
        
        total_count = db.get_tickets_count(
            status_filter=status_filter,
            priority_filter=priority_filter,
            search_query=search_query
        )
        
        return jsonify({
            'success': True,
            'tickets': tickets,
            'total': total_count,
            'page': page,
            'per_page': per_page
        })
        
    except Exception as e:
        logger.error(f\"Error getting tickets: {e}\")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('', methods=['POST'])
@ticket_bp.route('/', methods=['POST'])
def create_ticket():
    """Create a new support ticket."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
        data = request.get_json()
        
        # Validate required fields
        if not data.get('subject'):
            return jsonify({'success': False, 'error': 'Subject is required'}), 400
        if not data.get('body'):
            return jsonify({'success': False, 'error': 'Message body is required'}), 400
        
        # Sanitize inputs
        ticket_data = {
            'subject': sanitize_input(data.get('subject')),
            'body': sanitize_input(data.get('body')),
            'priority': data.get('priority', 'Medium'),
            'status': 'Open',
            'classification': data.get('classification', 'Support'),
            'email': data.get('email', ''),
            'name': data.get('name', session.get('member_name', 'Unknown')),
            'customer_name': data.get('customer_name', session.get('member_name', 'Unknown')),
            'created_by': session.get('member_id'),
            'source': 'manual',
            'is_important': False,
            'has_unread_reply': False
        }
        
        from database import get_db
        db = get_db()
        
        # Generate ticket ID
        ticket_id = db.generate_ticket_id()
        ticket_data['ticket_id'] = ticket_id
        
        # Create the ticket
        db.create_ticket(ticket_data)
        
        logger.info(f\"Ticket {ticket_id} created by {session.get('member_name')}\")
        
        return jsonify({
            'status': 'success',
            'success': True,
            'message': 'Ticket created successfully',
            'ticket_id': ticket_data['ticket_id'],
            'customer_number': ticket_data['ticket_id']
        })
        
    except Exception as e:
        logger.error(f\"Error creating ticket via API: {e}\")
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
        logger.error(f\"Error getting ticket {ticket_id}: {e}\")
        return jsonify({'success': False, 'error': str(e)}), 500


@ticket_bp.route('/<ticket_id>/reply-count', methods=['GET'])
def get_reply_count(ticket_id):
    """Get the count of replies for a ticket."""
    try:
        if not is_authenticated():
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
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
        logger.error(f\"Error getting reply count for {ticket_id}: {e}\")
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
        
        logger.info(f\"Ticket {ticket_id} status updated to {new_status} by {session.get('member_name')}\")
        
        return jsonify({
            'success': True,
            'message': f'Status updated to {new_status}',
            'ticket_id': ticket_id
        })
        
    except Exception as e:
        logger.error(f\"Error updating ticket status {ticket_id}: {e}\")
        return jsonify({'success': False, 'error': str(e)}), 500
