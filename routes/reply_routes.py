"""
Reply Attachment Routes (Backwards Compatibility)

These routes provide backwards compatibility for cached frontend code
that calls the old URL structure /api/replies/.../attachments/...

Author: AutoAssistGroup Development Team
"""

import logging
import os
from flask import Blueprint, jsonify
from bson.objectid import ObjectId
import base64

from middleware.session_manager import is_authenticated
from utils.file_utils import get_mime_type
from flask import make_response

logger = logging.getLogger(__name__)

# Create blueprint for reply routes
reply_bp = Blueprint('replies', __name__, url_prefix='/api/replies')


@reply_bp.route('/<reply_id>/attachments/<int:attachment_index>/download', methods=['GET'])
def download_reply_attachment_legacy(reply_id, attachment_index):
    """
    LEGACY ROUTE: Download attachment from a reply (backwards compatibility).
    Calls the same logic as the new /api/attachments/reply/{id}/{index} route.
    """
    try:
        if not is_authenticated():
            return jsonify({'error': 'Authentication required'}), 401
        
        from database import get_db
        db = get_db()
        
        # Get reply
        logger.info(f"[LEGACY DOWNLOAD] Looking for reply with ID: {reply_id}")
        reply = db.replies.find_one({'_id': ObjectId(reply_id)})
        if not reply:
            logger.error(f"[LEGACY DOWNLOAD] Reply not found: {reply_id}")
            return jsonify({'error': 'Reply not found'}), 404
        logger.info(f"[LEGACY DOWNLOAD] Reply found, has {len(reply.get('attachments', []))} attachments")
        
        # Get attachments
        attachments = reply.get('attachments', [])
        
        if attachment_index < 0 or attachment_index >= len(attachments):
            return jsonify({'error': 'Attachment not found'}), 404
        
        attachment = attachments[attachment_index]
        
        # Defensive: Ensure attachment is a dictionary
        if not isinstance(attachment, dict):
            logger.error(f"[LEGACY DOWNLOAD] Attachment {attachment_index} is not a dictionary: {type(attachment)}")
            return jsonify({'error': 'Invalid attachment format'}), 500
        
        # Get file data
        file_data = None
        filename = attachment.get('filename', attachment.get('fileName', 'download'))
        logger.info(f"[LEGACY DOWNLOAD] Attachment {attachment_index}: filename={filename}, has_data={bool(attachment.get('data'))}, has_fileData={bool(attachment.get('fileData'))}, file_path={attachment.get('file_path', 'NONE')}")
        
        # Try file_path first (reply attachments are saved to disk)
        if attachment.get('file_path') and os.path.exists(attachment.get('file_path')):
            try:
                with open(attachment['file_path'], 'rb') as f:
                    file_data = f.read()
                logger.info(f"[LEGACY DOWNLOAD] Read {len(file_data)} bytes from disk: {attachment['file_path']}")
            except Exception as e:
                logger.error(f"[LEGACY DOWNLOAD] Failed to read from disk: {e}")
        
        # Fallback to base64 data
        if not file_data and (attachment.get('data') or attachment.get('fileData')):
            base64_data = attachment.get('data') or attachment.get('fileData')
            try:
                file_data = base64.b64decode(base64_data)
                logger.info(f"[LEGACY DOWNLOAD] Successfully decoded {len(file_data)} bytes from base64")
            except Exception as e:
                logger.error(f"[LEGACY DOWNLOAD] Failed to decode base64 data: {e}")
        
        # 🚀 TICKET FALLBACK: If reply has no usable data, check the ticket's own attachments
        if not file_data:
            ticket_id = reply.get('ticket_id')
            if ticket_id:
                try:
                    ticket = db.tickets.find_one({'ticket_id': ticket_id})
                    if ticket:
                        ticket_atts = ticket.get('attachments', []) + ticket.get('simple_attachments', [])
                        for ta in ticket_atts:
                            ta_name = ta.get('filename', ta.get('name', ta.get('fileName', '')))
                            if ta_name == filename:
                                # Try disk path
                                ta_fp = ta.get('file_path', '')
                                if ta_fp and os.path.exists(ta_fp):
                                    with open(ta_fp, 'rb') as f:
                                        file_data = f.read()
                                    logger.info(f"[LEGACY DOWNLOAD] ✅ Fallback: read {len(file_data)} bytes from ticket att: {ta_fp}")
                                    break
                                # Try inline data
                                ta_data = ta.get('data', ta.get('fileData', ''))
                                if ta_data and len(str(ta_data)) >= 10:
                                    file_data = base64.b64decode(ta_data)
                                    logger.info(f"[LEGACY DOWNLOAD] ✅ Fallback: decoded {len(file_data)} bytes from ticket att base64")
                                    break
                except Exception as ticket_err:
                    logger.error(f"[LEGACY DOWNLOAD] Ticket fallback error: {ticket_err}")
        
        if not file_data:
            logger.error(f"[LEGACY DOWNLOAD] No attachment data available for {filename}")
            return jsonify({'error': 'Attachment data not available'}), 404
        
        mime_type = get_mime_type(filename)
        
        response = make_response(file_data)
        response.headers['Content-Type'] = mime_type
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response
        
    except Exception as e:
        logger.error(f"[LEGACY DOWNLOAD] Error downloading reply attachment: {e}")
        return jsonify({'error': str(e)}), 500


@reply_bp.route('/<reply_id>/attachments/<int:attachment_index>/preview', methods=['GET'])
def preview_reply_attachment_legacy(reply_id, attachment_index):
    """
    LEGACY ROUTE: Preview attachment from a reply inline (backwards compatibility).
    Calls the same logic as the new /api/attachments/reply/{id}/{index}/preview route.
    """
    try:
        if not is_authenticated():
            return jsonify({'error': 'Authentication required'}), 401
        
        from database import get_db
        db = get_db()
        
        # Get reply
        logger.info(f"[LEGACY PREVIEW] Looking for reply with ID: {reply_id}")
        reply = db.replies.find_one({'_id': ObjectId(reply_id)})
        if not reply:
            logger.error(f"[LEGACY PREVIEW] Reply not found: {reply_id}")
            return jsonify({'error': 'Reply not found'}), 404
        logger.info(f"[LEGACY PREVIEW] Reply found, has {len(reply.get('attachments', []))} attachments")
        
        # Get attachments
        attachments = reply.get('attachments', [])
        
        if attachment_index < 0 or attachment_index >= len(attachments):
            return jsonify({'error': 'Attachment not found'}), 404
        
        attachment = attachments[attachment_index]
        
        # Get file data
        file_data = None
        filename = attachment.get('filename', attachment.get('fileName', 'preview'))
        logger.info(f"[LEGACY PREVIEW] Attachment {attachment_index}: filename={filename}, has_data={bool(attachment.get('data'))}, has_fileData={bool(attachment.get('fileData'))}, file_path={attachment.get('file_path', 'NONE')}")
        
        # Try file_path first (reply attachments are saved to disk)
        if attachment.get('file_path') and os.path.exists(attachment.get('file_path')):
            try:
                with open(attachment['file_path'], 'rb') as f:
                    file_data = f.read()
                logger.info(f"[LEGACY PREVIEW] Read {len(file_data)} bytes from disk: {attachment['file_path']}")
            except Exception as e:
                logger.error(f"[LEGACY PREVIEW] Failed to read from disk: {e}")
        
        # Fallback to base64 data
        if not file_data and (attachment.get('data') or attachment.get('fileData')):
            base64_data = attachment.get('data') or attachment.get('fileData')
            try:
                file_data = base64.b64decode(base64_data)
                logger.info(f"[LEGACY PREVIEW] Successfully decoded {len(file_data)} bytes from base64")
            except Exception as e:
                logger.error(f"[LEGACY PREVIEW] Failed to decode base64: {e}")
        
        # 🚀 TICKET FALLBACK: If reply has no usable data, check the ticket's own attachments
        if not file_data:
            ticket_id = reply.get('ticket_id')
            if ticket_id:
                try:
                    ticket = db.tickets.find_one({'ticket_id': ticket_id})
                    if ticket:
                        ticket_atts = ticket.get('attachments', []) + ticket.get('simple_attachments', [])
                        for ta in ticket_atts:
                            ta_name = ta.get('filename', ta.get('name', ta.get('fileName', '')))
                            if ta_name == filename:
                                ta_fp = ta.get('file_path', '')
                                if ta_fp and os.path.exists(ta_fp):
                                    with open(ta_fp, 'rb') as f:
                                        file_data = f.read()
                                    logger.info(f"[LEGACY PREVIEW] ✅ Fallback: read {len(file_data)} bytes from ticket att: {ta_fp}")
                                    break
                                ta_data = ta.get('data', ta.get('fileData', ''))
                                if ta_data and len(str(ta_data)) >= 10:
                                    file_data = base64.b64decode(ta_data)
                                    logger.info(f"[LEGACY PREVIEW] ✅ Fallback: decoded {len(file_data)} bytes from ticket att base64")
                                    break
                except Exception as ticket_err:
                    logger.error(f"[LEGACY PREVIEW] Ticket fallback error: {ticket_err}")
        
        if not file_data:
            logger.error(f"[LEGACY PREVIEW] No attachment data available for {filename}")
            return jsonify({'error': 'Attachment data not available'}), 404
        
        mime_type = get_mime_type(filename)
        
        response = make_response(file_data)
        response.headers['Content-Type'] = mime_type
        response.headers['Content-Disposition'] = f'inline; filename="{filename}"'
        
        return response
        
    except Exception as e:
        logger.error(f"[LEGACY PREVIEW] Error previewing reply attachment: {e}")
        return jsonify({'error': str(e)}), 500
