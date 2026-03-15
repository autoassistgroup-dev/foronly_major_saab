"""
Email Template Routes Blueprint

This module handles the generation and retrieval of email templates
for the ticket detail view's email composition modal.

Author: AutoAssistGroup Development Team
"""

from flask import Blueprint, jsonify
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

email_template_bp = Blueprint('email_template', __name__, url_prefix='/api/email-template')


def generate_warranty_claim_template(ticket, customer_first_name):
    """Generate warranty claim template content"""
    ticket_id = ticket.get('ticket_id', '')
    return f"""Dear {customer_first_name},

Thank you for contacting Auto Assist Group regarding your warranty inquiry.

Ticket ID: #{ticket_id}

We have received your warranty claim and our Aftercare Team is reviewing the details. To process your claim efficiently, we may need some additional information:

• Vehicle registration number
• Current mileage reading (with dashboard photo)
• Any new fault codes or error messages
• Details of any recent services or repairs

Our warranty claim form is available at: https://autoassistgroup.com/report/claims

We will review your case within 2-3 business days and contact you with next steps. If your claim is approved, we will arrange the necessary remedial work at no cost to you.

If you have any questions in the meantime, please don't hesitate to contact us.

Best regards,
Auto Assist Group - Aftercare Team"""


def generate_technical_support_template(ticket, customer_first_name):
    """Generate technical support template content"""
    ticket_id = ticket.get('ticket_id', '')
    return f"""Dear {customer_first_name},

Thank you for reaching out regarding your technical issue.

Ticket ID: #{ticket_id}

We've received your inquiry and our technical team is reviewing the details. Based on the information provided, we will:

1. Assess the technical requirements for your vehicle
2. Provide you with a detailed solution and quote
3. Schedule the work at your convenience

Our technical specialists will contact you within 24 hours to discuss:
• Diagnostic findings and recommendations
• Service options and pricing
• Appointment availability

In the meantime, if you experience any urgent issues with your vehicle, please contact us immediately at 01234 567890.

Best regards,
Auto Assist Group - Technical Support Team"""


def generate_customer_service_template(ticket, customer_first_name):
    """Generate customer service template content"""
    ticket_id = ticket.get('ticket_id', '')
    return f"""Dear {customer_first_name},

Thank you for contacting Auto Assist Group.

Ticket ID: #{ticket_id}

We have received your inquiry and appreciate you choosing our services. Our customer service team is reviewing your request and will respond within 24 hours.

For immediate assistance, you can reach us at:
• Phone: 01234 567890 (Mon-Fri 8AM-6PM)
• Email: support@autoassistgroup.com

If you're looking to book a service, you can also use our online booking system at: https://autoassistgroup.com/book

We look forward to assisting you with your automotive needs.

Kind regards,
Auto Assist Group Customer Service Team"""


@email_template_bp.route('/<template_type>/<ticket_id>', methods=['GET'])
def get_email_template(template_type, ticket_id):
    """
    Get a specific email template populated with ticket data.
    
    Args:
        template_type: Type of template (warranty_claim, technical, etc.)
        ticket_id: ID of the ticket
        
    Returns:
        JSON with template data (subject, body, etc.)
    """
    try:
        from database import get_db
        db = get_db()
        
        # Get ticket details
        ticket = db.get_ticket_by_id(ticket_id)
        
        if not ticket:
            return jsonify({
                'status': 'error',
                'message': f'Ticket {ticket_id} not found'
            }), 404
            
        # Extract customer info
        customer_name = ticket.get('name', 'Customer').strip()
        first_name = customer_name.split()[0] if customer_name else 'Customer'
        
        # Determine subject
        original_subject = ticket.get('subject', 'Support Request')
        if not original_subject.lower().startswith('re:'):
            subject = f"Re: {original_subject} [TID: {ticket_id}]"
        else:
            subject = f"{original_subject} [TID: {ticket_id}]"
            
        # Check for existing draft
        draft = ticket.get('draft', '')
        has_draft = bool(draft)
        
        # Generate body based on template type
        body = ""
        content_source = "template"
        
        # NOTE: logic to prefer draft if highly relevant could go here,
        # but usually user selecting a template wants that specific template.
        
        if template_type == 'warranty_claim':
            body = generate_warranty_claim_template(ticket, first_name)
            subject = f"Re: Warranty Claim Update - Ticket #{ticket_id}"
            
        elif template_type == 'technical_support':
            body = generate_technical_support_template(ticket, first_name)
            
        elif template_type == 'customer_service':
            body = generate_customer_service_template(ticket, first_name)
            
        elif template_type == 'draft' and has_draft:
            # Explicit request for draft or fallback
            body = draft
            content_source = "draft"
            
            # Ensure ticket ID is present in draft if it's missing
            # This handles cases where a draft was saved without context
            if str(ticket_id) not in body and f"Ticket #{ticket_id}" not in body:
                # Add context header if missing
                context_header = f"Ref: Ticket #{ticket_id}\n\n"
                if not body.startswith("Ref: Ticket"):
                    body = context_header + body
            
        else:
            # Default fallback
            body = f"""Dear {first_name},

Thank you for contacting Auto Assist Group.

Ticket ID: #{ticket_id}

We have received your message and our team is reviewing it.

Best regards,
Auto Assist Group Support Team"""
        
        # Get attachments (only original ticket attachments, ignore claim docs and replies)
        raw_attachments = ticket.get('attachments', [])
        
        # 🚀 LIGHTWEIGHT METADATA ONLY — DO NOT send base64 data to the frontend!
        # The frontend only needs to know the attachment name, path, and whether 
        # data exists. The actual file reading happens in send_ticket_email when
        # the email is actually sent. Sending base64 to the frontend previously
        # caused DOM corruption (huge strings in data-* attributes broke HTML).
        import os
        resolved_attachments = []
        for att in raw_attachments:
            att_copy = dict(att)  # Don't modify the original
            att_name = att_copy.get('filename', att_copy.get('name', att_copy.get('fileName', '')))
            
            # Check if data exists (in DB or on disk) but do NOT send the actual data
            has_data = bool(att_copy.get('data') or att_copy.get('fileData'))
            file_path = att_copy.get('file_path', '')
            
            if not has_data and file_path and os.path.exists(file_path):
                # File exists on disk — mark as available but don't read it
                att_copy['has_data'] = True
                try:
                    att_copy['size'] = os.path.getsize(file_path)
                except:
                    pass
                logger.info(f"Attachment verified on disk: {att_name} (path: {file_path})")
            elif has_data:
                att_copy['has_data'] = True
            else:
                att_copy['has_data'] = False
                logger.warning(f"Attachment has no data and no valid file_path: {att_name}, path={file_path}")
            
            # 🔥 CRITICAL: Strip base64 data before sending to frontend
            # This prevents DOM corruption from huge strings in HTML data attributes
            att_copy.pop('data', None)
            att_copy.pop('fileData', None)
            att_copy.pop('content', None)
            
            # Ensure name field is set
            if not att_copy.get('name'):
                att_copy['name'] = att_name
            
            resolved_attachments.append(att_copy)
        
        logger.info(f"Email template for {ticket_id}: {len(resolved_attachments)} attachments resolved")
        
        return jsonify({
            'status': 'success',
            'template': {
                'ticket_id': ticket_id,
                'subject': subject,
                'body': body,
                'attachments': resolved_attachments,
                'has_draft': has_draft,
                'content_source': content_source,
                'template_type': template_type
            }
        })
        
    except Exception as e:
        logger.error(f"Error generating email template: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
