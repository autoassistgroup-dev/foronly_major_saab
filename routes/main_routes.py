"""
Main Page Routes

Handles all page routes for the application including:
- Index/Tickets list
- Dashboard
- Ticket detail
- Create ticket
- Status page
- Members page
- Technicians page
- Admin panel
- Tech Director dashboard

Author: AutoAssistGroup Development Team
"""

import logging
from datetime import datetime, timedelta
from flask import Blueprint, render_template, redirect, url_for, request, flash, session, jsonify
from middleware.session_manager import safe_member_lookup, is_authenticated, is_admin, get_current_user_id

logger = logging.getLogger(__name__)


def _is_admin_role(role):
    """True if role is Administrator or Admin (any casing)."""
    if not role:
        return False
    return str(role).strip().lower() in ('administrator', 'admin')

main_bp = Blueprint('main', __name__)

@main_bp.route('/api/test_db_direct')
def test_db_direct():
    try:
        from database import get_db
        db = get_db()
        import traceback
        
        output = "=== DATABASE DEBUG INFO ===\n\n"
        output += f"Total tickets in DB: {db.tickets.count_documents({})}\n\n"
        
        output += "Testing database method get_tickets_with_assignments:\n"
        try:
            # Call the actual method that is failing
            tickets = db.get_tickets_with_assignments(page=1, per_page=10)
            output += f"Method returned a list of length: {len(tickets)}\n\n"
            if len(tickets) > 0:
                output += f"First ticket ID: {tickets[0].get('ticket_id')}\n"
            
            if hasattr(db, "last_error") and db.last_error:
                output += f"Method failed internally with error:\n{db.last_error}\n"
        except Exception as e:
            output += f"Method raised exception:\n{str(e)}\n{traceback.format_exc()}\n\n"
            
        return f"<pre>{output}</pre>"
        
    except Exception as e:
        import traceback
        return f"<pre>Fatal API error:\n{str(e)}\n{traceback.format_exc()}</pre>"

import uuid
from werkzeug.security import generate_password_hash
from bson.objectid import ObjectId


def _sanitize_ticket_for_template(ticket):
    """
    Ensure ticket has safe values for Jinja (n8n/API tickets may have None or missing fields).
    Returns a copy with defaults so template never sees None where it expects string/list.
    """
    if not ticket:
        return ticket
    out = dict(ticket)
    out.pop('raw_data', None)  # avoid passing large/non-serializable n8n payload to template
    out['subject'] = out.get('subject') or 'No Subject'
    out['body'] = out.get('body') or out.get('message') or out.get('description') or ''
    out['message'] = out.get('message') or out.get('body') or out.get('description') or ''
    out['description'] = out.get('description') or out.get('body') or out.get('message') or ''
    out['draft_body'] = out.get('draft_body') or out.get('n8n_draft') or out.get('draft') or ''
    atts = out.get('attachments')
    if not isinstance(atts, list):
        out['attachments'] = []
    else:
        out['attachments'] = []
        for a in atts:
            if not isinstance(a, dict):
                continue
            na = dict(a)
            na['filename'] = a.get('filename') or a.get('fileName') or a.get('name') or 'attachment'
            out['attachments'].append(na)
    simple = out.get('simple_attachments')
    if not isinstance(simple, list):
        out['simple_attachments'] = []
    else:
        out['simple_attachments'] = []
        for a in simple:
            if not isinstance(a, dict):
                continue
            na = dict(a)
            na['fileName'] = a.get('fileName') or a.get('filename') or a.get('name') or 'attachment'
            out['simple_attachments'].append(na)
    return out


@main_bp.route('/')
def home():
    """Root route - redirects based on authentication status."""
    # If user is logged in, go to tickets; otherwise go to portal
    if is_authenticated():
        return redirect(url_for('main.index'))  # Tickets page
    return redirect(url_for('main.portal'))


@main_bp.route('/tickets')
def index():
    """Main tickets list page."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        return redirect(url_for('auth.login'))
    
    from database import get_db
    db = get_db()
    
    # Get pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    status_filter = request.args.get('status', 'All')
    priority_filter = request.args.get('priority', 'All')
    search_query = request.args.get('search', '')
    
    # Technical Director can ONLY see referred/forwarded tickets
    is_tech_director = current_member.get('role') == 'Technical Director'
    current_member_id = session.get('member_id')
    
    # For Tech Director: Show forwarded tickets as the main ticket list
    if is_tech_director and current_member_id:
        forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id)
        if search_query:
            search_lower = search_query.lower()
            forwarded_tickets = [t for t in forwarded_tickets if 
                search_lower in (t.get('ticket_id', '') or '').lower() or
                search_lower in (t.get('subject', '') or '').lower() or
                search_lower in (t.get('name', '') or '').lower() or
                search_lower in (t.get('email', '') or '').lower()
            ]
        if priority_filter and priority_filter != 'All':
            forwarded_tickets = [t for t in forwarded_tickets if t.get('priority') == priority_filter]
        total_count = len(forwarded_tickets)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        tickets = forwarded_tickets[start_idx:end_idx]
        
    else:
        # Tickets forwarded TO this user (shown in "Forwarded to You" section)
        # Filter out Closed tickets so actioned ones auto-disappear
        if current_member_id:
            forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id)
            # Remove actioned (Closed) tickets from forwarded section
            forwarded_tickets = [t for t in forwarded_tickets if t.get('status') != 'Closed']
        else:
            forwarded_tickets = []
        forwarded_ids = [t['ticket_id'] for t in forwarded_tickets] if forwarded_tickets else []
        tickets = db.get_tickets_with_assignments(
            page=page, 
            per_page=per_page,
            status_filter=status_filter if status_filter != 'All' else None,
            priority_filter=priority_filter if priority_filter != 'All' else None,
            search_query=search_query if search_query else None,
            referred_only=False,
            exclude_ids=forwarded_ids
        )
        total_count = db.get_tickets_count(
            status_filter=status_filter if status_filter != 'All' else None,
            priority_filter=priority_filter if priority_filter != 'All' else None,
            search_query=search_query if search_query else None,
            referred_only=False,
            exclude_ids=forwarded_ids
        )
    
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    
    members = db.get_all_members()
    technicians = db.get_all_technicians()
    ticket_statuses = list(db.ticket_statuses.find({"is_active": True}).sort("order", 1))
    
    # Optimized Stats Loading
    ticket_stats = db.get_ticket_stats()
    
    # Extract stats from optimized result
    priorities = ticket_stats.get('priorities', {'Urgent': 0, 'Fast': 0, 'High': 0, 'Medium': 0, 'Low': 0})
    classifications = ticket_stats.get('classifications', {})
    status_counts = ticket_stats.get('status_counts', {})
    
    # Calculate derived metrics from status counts
    open_tickets = 0
    waiting_tickets = 0
    resolved_tickets = 0
    
    for status, count in status_counts.items():
        if status in ['Open', 'New', 'Reopened']:
            open_tickets += count
        elif 'Waiting' in status:
            waiting_tickets += count
        elif status in ['Resolved', 'Closed']:
            resolved_tickets += count
    
    total_tickets = ticket_stats.get('total_tickets', 0)
    
    pagination = {
        'current_page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'total_count': total_count,
        'total_tickets': total_tickets,
        'has_prev': page > 1,
        'has_next': page < total_pages,
        'prev_page': page - 1 if page > 1 else None,
        'next_page': page + 1 if page < total_pages else None,
        'status_filter': status_filter,
        'priority_filter': priority_filter,
        'search_query': search_query
    }
    
    return render_template('index.html',
                          tickets=tickets,
                          all_tickets=tickets, # Keep for compatibility, but it's just the page now
                          current_member=current_member,
                          current_user=current_member.get('name') or session.get('member_name') or 'User',
                          current_user_role=current_member.get('role') or session.get('member_role') or 'User',
                          is_tech_director=is_tech_director,
                          members=members,
                          technicians=technicians,
                          ticket_statuses=ticket_statuses,
                          priorities=priorities,
                          classifications=classifications,
                          status_counts=status_counts,
                          total_tickets=total_tickets,
                          open_tickets=open_tickets,
                          waiting_tickets=waiting_tickets,
                          resolved_tickets=resolved_tickets,
                          forwarded_tickets=forwarded_tickets,
                          pagination=pagination)


@main_bp.route('/api/index/tickets', methods=['GET'])
def api_index_tickets():
    """
    JSON API for the tickets index page.
    Returns both forwarded tickets and regular tickets with essential fields for AJAX syncing.
    """
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401

    current_member = safe_member_lookup()
    if not current_member:
        return jsonify({'success': False, 'error': 'Authentication required'}), 401

    from database import get_db
    db = get_db()

    # Reuse same filters as index() for consistency
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    status_filter = request.args.get('status', 'All')
    priority_filter = request.args.get('priority', 'All')
    search_query = request.args.get('search', '')

    is_tech_director = current_member.get('role') == 'Technical Director'
    current_member_id = session.get('member_id')

    # Helper to normalize a ticket for JSON
    def serialize_ticket(ticket):
        created_at = ticket.get('created_at')
        updated_at = ticket.get('updated_at')
        # Extract forwarded_to_name from lookup results or direct field
        forwarded_to_name = ''
        if ticket.get('forwarded_to_member') and len(ticket['forwarded_to_member']) > 0:
            forwarded_to_name = ticket['forwarded_to_member'][0].get('name', '')
        elif ticket.get('forwarded_to_name'):
            forwarded_to_name = ticket['forwarded_to_name']
        return {
            'ticket_id': ticket.get('ticket_id'),
            'ticket_number': ticket.get('ticket_id'),
            'subject': ticket.get('subject') or 'No Subject',
            'body': ticket.get('body') or ticket.get('message') or ticket.get('description') or '',
            'name': ticket.get('name') or ticket.get('customer_first_name') or '',
            'email': ticket.get('email') or '',
            'priority': ticket.get('priority') or 'Medium',
            'status': ticket.get('status') or 'New',
            'classification': ticket.get('classification'),
            'is_forwarded': bool(ticket.get('is_forwarded')),
            'is_forwarded_viewed': bool(ticket.get('is_forwarded_viewed')),
            'is_new_viewed': bool(ticket.get('is_new_viewed', True)),  # Default True if missing
            'has_unread_notification': bool(ticket.get('has_unread_notification', False)),
            'referred_back_by_name': ticket.get('referred_back_by_name'),
            'forwarded_to_name': forwarded_to_name,
            'assigned_technician_name': ticket.get('assigned_technician') or ticket.get('technician_name') or '',
            'is_bookmarked': bool(ticket.get('is_important')),
            'has_new_reply': bool(ticket.get('has_unread_reply')),
            'created_at': created_at.isoformat() if isinstance(created_at, datetime) else None,
            'updated_at': updated_at.isoformat() if isinstance(updated_at, datetime) else None,
        }

    if is_tech_director and current_member_id:
        forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id)
        if search_query:
            search_lower = search_query.lower()
            forwarded_tickets = [t for t in forwarded_tickets if
                                 search_lower in (t.get('ticket_id', '') or '').lower() or
                                 search_lower in (t.get('subject', '') or '').lower() or
                                 search_lower in (t.get('name', '') or '').lower() or
                                 search_lower in (t.get('email', '') or '').lower()
                                 ]
        if priority_filter and priority_filter != 'All':
            forwarded_tickets = [t for t in forwarded_tickets if t.get('priority') == priority_filter]
        tickets = forwarded_tickets
        regular_tickets = []
    else:
        # Tickets forwarded TO this user, excluding Closed (actioned) ones
        if current_member_id:
            forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id)
            forwarded_tickets = [t for t in forwarded_tickets if t.get('status') != 'Closed']
        else:
            forwarded_tickets = []
        forwarded_ids = [t['ticket_id'] for t in forwarded_tickets] if forwarded_tickets else []
        
        regular_tickets = db.get_tickets_with_assignments(
            page=page,
            per_page=per_page,
            status_filter=status_filter if status_filter != 'All' else None,
            priority_filter=priority_filter if priority_filter != 'All' else None,
            search_query=search_query if search_query else None,
            referred_only=False,
            exclude_ids=forwarded_ids
        )
        tickets = forwarded_tickets + regular_tickets

    return jsonify({
        'success': True,
        'tickets': [serialize_ticket(t) for t in tickets],
        'forwarded_tickets': [serialize_ticket(t) for t in forwarded_tickets],
        'regular_tickets': [serialize_ticket(t) for t in regular_tickets],
    })


@main_bp.route('/dashboard')
def dashboard():
    """Main dashboard with full analytics."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        return redirect(url_for('auth.login'))
    
    from database import get_db
    db = get_db()
    
    # Technical Director can ONLY see referred tickets
    is_tech_director = current_member.get('role') == 'Technical Director'
    
    # Get base data
    tickets = db.get_tickets_with_assignments(page=1, per_page=50, referred_only=is_tech_director)
    members = db.get_all_members()
    technicians = list(db.technicians.find({"is_active": True}))
    ticket_statuses = list(db.ticket_statuses.find({"is_active": True}).sort("order", 1))
    
    # Optimized Dashboard Stats (using ticket stats instead of warranty claims)
    ticket_stats = db.get_ticket_stats()
    status_counts = ticket_stats.get('status_counts', {})
    priority_counts = ticket_stats.get('priorities', {})
    total_tickets = ticket_stats.get('total_tickets', 0)
    
    active_tickets = 0
    waiting_tickets = 0
    
    for status, count in status_counts.items():
        if status not in ['Closed', 'Resolved']:
            active_tickets += count
        if 'Waiting' in status:
            waiting_tickets += count
            
    # "Resolved Today" requires a specific date query or aggregation we can add later.
    # For now, let's keep it 0 or add a lightweight query if needed. 
    resolved_today = 0
    
    from utils.date_utils import safe_date_format
    for ticket in tickets:
        ticket['formatted_date'] = safe_date_format(ticket.get('created_at')) or 'Unknown'
    
    return render_template('dashboard.html',
                          tickets=tickets,
                          recent_tickets=tickets, # Reuse the tickets list for the recent table
                          current_member=current_member,
                          current_user=current_member.get('name') or session.get('member_name') or 'User',
                          current_user_role=current_member.get('role') or session.get('member_role') or 'User',
                          members=members,
                          technicians=technicians,
                          ticket_statuses=ticket_statuses,
                          status_counts=status_counts,
                          priority_counts=priority_counts,
                          total_tickets=total_tickets,
                          active_tickets=active_tickets,
                          waiting_tickets=waiting_tickets,
                          resolved_today=resolved_today,
                          pagination=None)


@main_bp.route('/ticket/<ticket_id>')
def ticket_detail(ticket_id):
    """View single ticket details."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        return redirect(url_for('auth.login'))
    
    from database import get_db
    db = get_db()
    
    ticket = db.get_ticket_by_id(ticket_id)
    if not ticket:
        flash('Ticket not found', 'error')
        return redirect(url_for('main.index'))
    
    # Mark ticket as viewed (clears has_unread_notification)
    db.mark_ticket_viewed(ticket_id)
    
    # Sanitize ticket for template (n8n/API tickets may have None or malformed fields)
    ticket = _sanitize_ticket_for_template(ticket)
        
    # Check if this is a forwarded ticket for the current user and mark as viewed
    # We use string comparison for IDs to handle both ObjectId and string formats robustly
    if ticket.get('is_forwarded') and str(ticket.get('forwarded_to')) == str(current_member.get('_id')):
        if not ticket.get('is_forwarded_viewed'):
            db.mark_forwarded_ticket_viewed(ticket_id, current_member.get('_id'))
            # Update local object for this render
            ticket['is_forwarded_viewed'] = True
    
    replies = db.get_replies_by_ticket(ticket_id)
    members = db.get_all_members()
    technicians = db.get_all_technicians()
    ticket_statuses = list(db.ticket_statuses.find({"is_active": True}).sort("order", 1))
    
    is_tech_director = current_member.get('role') == 'Technical Director'
    
    from utils.date_utils import safe_date_format
    formatted_date = safe_date_format(ticket.get('created_at')) or 'Unknown'
    
    return render_template('ticket_detail.html',
                          ticket=ticket,
                          replies=replies,
                          current_member=current_member,
                          current_user=current_member.get('name') or 'User',
                          current_user_role=current_member.get('role') or 'User',
                          is_tech_director=is_tech_director,
                          
                          # Unpack common ticket fields for template convenience
                          customer_title=ticket.get('customer_title'),
                          customer_first_name=ticket.get('customer_first_name'),
                          customer_surname=ticket.get('customer_surname'),
                          vehicle_registration=ticket.get('vehicle_registration'),
                          service_date=ticket.get('service_date'),
                          claim_date=ticket.get('claim_date'),
                          type_of_claim=ticket.get('type_of_claim'),
                          days_between_service_claim=ticket.get('days_between_service_claim'),
                          vhc_link=ticket.get('vhc_link'),
                          
                          # Checklist fields
                          advisories_followed=ticket.get('advisories_followed'),
                          within_warranty=ticket.get('within_warranty'),
                          new_fault_codes=ticket.get('new_fault_codes'),
                          dpf_light_on=ticket.get('dpf_light_on'),
                          eml_light_on=ticket.get('eml_light_on'),
                          
                          # Technician details
                          technician_name=ticket.get('assigned_technician'),
                          technician_id=ticket.get('technician_id') or ticket.get('assigned_technician_id'),
                          
                          # Outcome details
                          outcome_category=ticket.get('outcome_category'),
                          outcome_notes=ticket.get('outcome_notes'),
                          revisit_carried_out=ticket.get('revisit_carried_out'),
                          clean_under_warranty=ticket.get('clean_under_warranty'),
                          
                          members=members,
                          technicians=technicians,
                          ticket_statuses=ticket_statuses,
                          formatted_date=formatted_date)


@main_bp.route('/create-ticket', methods=['GET', 'POST'])
def create_ticket():
    """Create a new ticket."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        return redirect(url_for('auth.login'))
    
    from database import get_db
    import uuid
    db = get_db()
    
    if request.method == 'POST':
        # Generate ticket ID first to use in thread_id
        ticket_id = 'M' + str(uuid.uuid4())[:5].upper()
        
        ticket_data = {
            'ticket_id': ticket_id,
            'subject': request.form.get('subject', ''),
            'body': request.form.get('body', '') or request.form.get('description', ''),
            'name': request.form.get('name', current_member.get('name', '')),
            'email': request.form.get('email', ''),
            'phone': request.form.get('phone', ''),
            'vhc_link': request.form.get('vhc_link', '').strip(),
            'status': 'New',
            'priority': request.form.get('priority', 'Medium'),
            'created_at': datetime.now(),
            'creation_method': 'manual'
        }
        
        try:
            db.create_ticket(ticket_data)
            
            # Emit real-time notification
            try:
                from socket_events import emit_new_ticket
                emit_new_ticket({
                    'ticket_id': ticket_data['ticket_id'],
                    'subject': ticket_data.get('subject', 'No Subject'),
                    'name': ticket_data.get('name', 'Anonymous'),
                    'email': ticket_data.get('email', ''),
                    'priority': ticket_data.get('priority', 'Medium'),
                    'status': 'New',
                    'created_at': ticket_data.get('created_at').isoformat() if ticket_data.get('created_at') else None,
                    'is_manual': True
                })
            except Exception as e:
                # Log but don't fail the request
                print(f"Failed to emit socket event: {e}")
                
            flash('Ticket created successfully!', 'success')
            return redirect(url_for('main.ticket_detail', ticket_id=ticket_data['ticket_id']))
        except Exception as e:
            flash(f'Error creating ticket: {e}', 'error')
    
    technicians = list(db.technicians.find({"is_active": True}))
    
    return render_template('create_ticket.html',
                          current_member=current_member,
                          current_user=current_member.get('name') or 'User',
                          current_user_role=current_member.get('role') or 'User',
                          technicians=technicians)





@main_bp.route('/members')
def members_page():
    """Members management page."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member or current_member.get('role') != 'Administrator':
        flash('Access denied', 'error')
        return redirect(url_for('main.index'))
    
    from database import get_db
    db = get_db()
    
    members = db.get_all_members()
    
    return render_template('members.html',
                          current_member=current_member,
                          current_user=current_member.get('name') or session.get('member_name') or 'User',
                          current_user_role=current_member.get('role') or session.get('member_role') or 'User',
                          members=members)


@main_bp.route('/members/add', methods=['POST'])
def add_member():
    """Add a new team member."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
        
    current_member = safe_member_lookup()
    if not current_member or current_member.get('role') != 'Administrator':
        flash('Access denied', 'error')
        return redirect(url_for('main.index'))
        
    from database import get_db
    db = get_db()
    
    name = request.form.get('name')
    email = request.form.get('email')
    password = request.form.get('password')
    role = request.form.get('role')
    gender = request.form.get('gender')
    
    if not name or not email or not password or not role:
        flash('All required fields must be filled', 'error')
        return redirect(url_for('main.members_page'))
        
    try:
        # Generate user_id from email prefix or name if email is malformed
        user_id = email.split('@')[0].lower() if '@' in email else name.lower().replace(' ', '')
        
        member_data = {
            'name': name,
            'email': email,
            'user_id': user_id,
            'password_hash': generate_password_hash(password),
            'role': role,
            'gender': gender,
            'is_active': True,
            'created_at': datetime.now()
        }
        
        db.create_member(member_data)
        flash(f'Member {name} added successfully!', 'success')
    except ValueError as e:
        flash(str(e), 'error')
    except Exception as e:
        flash(f'Error adding member: {e}', 'error')
        
    return redirect(url_for('main.members_page'))


@main_bp.route('/members/edit', methods=['POST'])
def edit_member():
    """Edit an existing team member."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
        
    current_member = safe_member_lookup()
    if not current_member or current_member.get('role') != 'Administrator':
        flash('Access denied', 'error')
        return redirect(url_for('main.index'))
        
    from database import get_db
    db = get_db()
    
    member_id = request.form.get('member_id')
    name = request.form.get('name')
    email = request.form.get('email')
    password = request.form.get('password')
    role = request.form.get('role')
    gender = request.form.get('gender')
    
    if not member_id:
        flash('Member ID is missing', 'error')
        return redirect(url_for('main.members_page'))
        
    try:
        update_data = {
            'name': name,
            'email': email,
            'role': role,
            'gender': gender,
            'updated_at': datetime.now()
        }
        
        # Only update password if provided
        if password:
            update_data['password_hash'] = generate_password_hash(password)
            
        db.members.update_one(
            {'_id': ObjectId(member_id)},
            {'$set': update_data}
        )
        flash(f'Member {name} updated successfully!', 'success')
    except Exception as e:
        flash(f'Error updating member: {e}', 'error')
        
    return redirect(url_for('main.members_page'))


@main_bp.route('/members/delete/<member_id>', methods=['POST'])
def delete_member(member_id):
    """Delete (deactivate) a team member."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
        
    current_member = safe_member_lookup()
    if not current_member or current_member.get('role') != 'Administrator':
        flash('Access denied', 'error')
        return redirect(url_for('main.index'))
        
    from database import get_db
    db = get_db()
    
    try:
        # Check if trying to delete self or default admin
        member = db.get_member_by_id(member_id)
        if member:
            if member.get('email') == 'admin@autoassist.com':
                flash('Cannot delete protected admin account', 'error')
            elif str(member.get('_id')) == str(current_member.get('_id')):
                flash('Cannot delete your own account', 'error')
            else:
                db.members.update_one(
                    {'_id': ObjectId(member_id)},
                    {'$set': {'is_active': False, 'deleted_at': datetime.now()}}
                )
                flash('Member deactivated successfully', 'success')
        else:
            flash('Member not found', 'error')
    except Exception as e:
        flash(f'Error deleting member: {e}', 'error')
        
    return redirect(url_for('main.members_page'))


@main_bp.route('/technicians')
def technicians_page():
    """Technicians management page."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member or current_member.get('role') != 'Administrator':
        flash('Access denied', 'error')
        return redirect(url_for('main.index'))
    
    from database import get_db
    db = get_db()
    
    technicians = list(db.technicians.find())
    
    # Use current_member for display (session imported at top of module)
    current_user = current_member.get('name') or 'User'
    current_user_role = current_member.get('role') or 'User'
    
    # DEBUG: Log values being passed to template
    print(f"🔍 TECHNICIANS DEBUG: current_member={current_member}")
    # Use standard template
    return render_template('technicians.html',
                          current_member=current_member,
                          technicians=technicians)


@main_bp.route('/admin')
def admin_panel():
    """Admin panel page."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        flash('Session expired or user not found. Please log in again.', 'error')
        return redirect(url_for('auth.login'))
    if not _is_admin_role(current_member.get('role')) and not is_admin():
        flash('Access denied. Administrator access required.', 'error')
        return redirect(url_for('main.index'))
    
    from database import get_db
    db = get_db()
    
    members = db.get_all_members()
    technicians_raw = list(db.technicians.find())
    ticket_statuses_raw = list(db.ticket_statuses.find().sort("order", 1))
    roles_raw = list(db.roles.find())
    
    # Convert ObjectId to string for JSON serialization in templates (preserve datetime for strftime)
    def serialize_doc(doc):
        """Convert MongoDB document ObjectId fields to strings. Preserves datetime for template use."""
        if doc is None:
            return None
        serialized = {}
        for key, value in doc.items():
            if hasattr(value, '__str__') and type(value).__name__ == 'ObjectId':
                serialized[key] = str(value)
            elif isinstance(value, dict):
                serialized[key] = serialize_doc(value)
            elif isinstance(value, list):
                serialized[key] = [serialize_doc(item) if isinstance(item, dict) else item for item in value]
            else:
                serialized[key] = value
        return serialized
    
    technicians = [serialize_doc(t) for t in technicians_raw]
    ticket_statuses = [serialize_doc(s) for s in ticket_statuses_raw]
    roles = [serialize_doc(r) for r in roles_raw]
    members = [serialize_doc(m) for m in members]
    
    # Optimized Admin Stats
    ticket_stats = db.get_ticket_stats()
    
    priorities = ticket_stats.get('priorities', {'Urgent': 0, 'Fast': 0, 'High': 0, 'Medium': 0, 'Low': 0})
    classifications = ticket_stats.get('classifications', {})
    status_counts = ticket_stats.get('status_counts', {})
    
    open_tickets = 0
    resolved_tickets = 0
    active_tickets = 0
    waiting_tickets = 0
    
    # Fetch recent tickets for the table
    tickets = db.get_tickets_with_assignments(page=1, per_page=50)
    
    # CRITICAL FIX: Admin must see tickets forwarded to them
    # Fetch forwarded tickets using both possible member ID sources (DB may store ObjectId or string)
    # Use get_current_user_id() to avoid referencing session inside this function (prevents UnboundLocalError)
    current_member_id = str(current_member.get('_id')) if current_member.get('_id') else None
    session_member_id = get_current_user_id()
    
    forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id or session_member_id)
    
    # If no results, try the other ID in case DB has the other format
    if not forwarded_tickets and session_member_id and str(session_member_id) != str(current_member_id):
        forwarded_tickets = db.get_forwarded_tickets_to_user(session_member_id)
    
    # Merge forwarded tickets with regular tickets, avoiding duplicates
    existing_ticket_ids = {t.get('ticket_id') for t in tickets if t.get('ticket_id')}
    for forwarded_ticket in forwarded_tickets:
        ticket_id = forwarded_ticket.get('ticket_id')
        if ticket_id and ticket_id not in existing_ticket_ids:
            tickets.append(forwarded_ticket)
            existing_ticket_ids.add(ticket_id)

    for status, count in status_counts.items():
        if status in ['Resolved', 'Closed']:
            resolved_tickets += count
        else:
            active_tickets += count
            if status in ['Open', 'New', 'Reopened']:
                open_tickets += count
            if 'Waiting' in status:
                waiting_tickets += count
    
    total_tickets = ticket_stats.get('total_tickets', 0)
    
    # Format dates for display (same as dashboard)
    from utils.date_utils import safe_date_format
    for ticket in tickets:
        ticket['formatted_date'] = safe_date_format(ticket.get('created_at')) or 'Unknown'
    
    # Use current_member for display (no session reference here to avoid UnboundLocalError)
    current_user = current_member.get('name') or 'User'
    current_user_role = current_member.get('role') or 'User'
    
    return render_template('admin.html',
                          current_member=current_member,
                          current_user=current_user,
                          current_user_role=current_user_role,
                          members=members,
                          technicians=technicians,
                          ticket_statuses=ticket_statuses,
                          roles=roles,
                          priorities=priorities,
                          classifications=classifications,
                          status_counts=status_counts,
                          tickets=tickets,
                          total_tickets=total_tickets,
                          open_tickets=open_tickets,
                          resolved_tickets=resolved_tickets,
                          active_tickets=active_tickets,
                          waiting_tickets=waiting_tickets)


@main_bp.route('/tech-director-dashboard')
def tech_director_dashboard():
    """Technical Director dashboard."""
    if not is_authenticated():
        return redirect(url_for('auth.login'))
    
    current_member = safe_member_lookup()
    if not current_member:
        return redirect(url_for('auth.login'))
    
    from database import get_db
    db = get_db()
    
    current_member_id = str(current_member.get('_id'))
    forwarded_tickets = db.get_forwarded_tickets_to_user(current_member_id)
    total_referred = len(forwarded_tickets)
    try:
        from bson.objectid import ObjectId
        member_id_obj = ObjectId(current_member_id) if isinstance(current_member_id, str) else current_member_id
        forwarded_to_others = db.tickets.count_documents({
            "forwarded_by": member_id_obj,
            "is_forwarded": True
        })
    except Exception as e:
        logger.warning(f"[TECH_DIRECTOR_DASHBOARD] Error counting forwarded_to_others: {e}")
        forwarded_to_others = 0
    
    # Get resolved/closed tickets that were previously referred to TD
    try:
        resolved_tickets = list(db.tickets.find({
            "status": {"$in": ["Resolved", "Closed"]},
            "referred_to_director": True,
            "td_cleared": {"$ne": True}
        }).sort("updated_at", -1))
    except Exception as e:
        logger.warning(f"[TECH_DIRECTOR_DASHBOARD] Error getting resolved tickets: {e}")
        resolved_tickets = []
    
    members = db.get_all_members()
    
    return render_template('tech_director_dashboard.html',
                          current_member=current_member,
                          current_user=current_member.get('name') or session.get('member_name') or 'User',
                          current_user_role=current_member.get('role') or session.get('member_role') or 'User',
                          tickets=forwarded_tickets,
                          referred_tickets=forwarded_tickets,
                          resolved_tickets=resolved_tickets,
                          total_referred=total_referred,
                          forwarded_to_others=forwarded_to_others,
                          members=members)


@main_bp.route('/portal')
def portal():
    """Portal page."""
    return render_template('portal.html')
