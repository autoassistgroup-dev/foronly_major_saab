"""
Common Document Routes

Handles operations for common/shared documents including:
- Listing documents
- Uploading new documents
- Updating document metadata
- Downloading documents
- Deleting documents

Author: AutoAssistGroup Development Team
"""

import os
import logging
import base64
import mimetypes
from datetime import datetime
from flask import Blueprint, jsonify, request, send_file, make_response
from werkzeug.utils import secure_filename
from bson.objectid import ObjectId

from middleware.session_manager import is_authenticated, is_admin
from config.settings import Config

logger = logging.getLogger(__name__)

common_docs_bp = Blueprint('common_docs', __name__)


def allowed_file(filename):
    """Check if file extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in Config.ALLOWED_EXTENSIONS


@common_docs_bp.route('/api/common-documents', methods=['GET'])
def list_documents():
    """Get all common documents."""
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
    
    from database import get_db
    db = get_db()
    
    try:
        # Fetch documents, sort by created_at desc
        documents = list(db.common_documents.find().sort("created_at", -1))
        
        # Serialize for JSON
        serialized_docs = []
        for doc in documents:
            serialized_docs.append({
                '_id': str(doc.get('_id')),
                'name': doc.get('name'),
                'type': doc.get('type'),
                'description': doc.get('description', ''),
                'file_name': doc.get('file_name'),
                'file_size': doc.get('file_size', 0),
                'created_at': doc.get('created_at').isoformat() if doc.get('created_at') else None,
                'updated_at': doc.get('updated_at').isoformat() if doc.get('updated_at') else None,
                'is_active': doc.get('is_active', True)
            })
            
        return jsonify({
            'success': True, 
            'documents': serialized_docs
        })
        
    except Exception as e:
        logger.error(f"Error listing common documents: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@common_docs_bp.route('/api/common-documents', methods=['POST'])
def create_document():
    """Upload a new common document."""
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
    
    # Typically only admins or certain roles might upload, but for now allow authenticated
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file part'}), 400
        
    file = request.files['file']
    name = request.form.get('name')
    doc_type = request.form.get('type', 'form')
    description = request.form.get('description', '')
    
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No selected file'}), 400
        
    if not name:
        return jsonify({'success': False, 'message': 'Document name is required'}), 400
        
    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            upload_folder = Config.get_upload_folder()
            
            # Create common_docs subfolder to keep organized
            # common_docs_folder = os.path.join(upload_folder, 'common_docs')
            # os.makedirs(common_docs_folder, exist_ok=True)
            # Actually, let's keep it simple and use the main bucket but maybe prefix?
            
            # Generate unique filename to avoid collisions
            unique_filename = f"common_{int(datetime.now().timestamp())}_{filename}"
            file_path = os.path.join(upload_folder, unique_filename)
            
            file.save(file_path)
            file_size = os.path.getsize(file_path)
            
            from database import get_db
            db = get_db()
            
            document_data = {
                'name': name,
                'type': doc_type,
                'description': description,
                'file_name': filename,
                'file_path': file_path,
                'file_size': file_size,
                'is_active': True,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'uploaded_by': request.form.get('uploaded_by', 'system') # Could get from session
            }
            
            result = db.common_documents.insert_one(document_data)
            
            return jsonify({
                'success': True,
                'message': 'Document uploaded successfully',
                'document_id': str(result.inserted_id)
            })
            
        except Exception as e:
            logger.error(f"Error uploading document: {e}")
            return jsonify({'success': False, 'message': f"Upload failed: {str(e)}"}), 500
    else:
        return jsonify({'success': False, 'message': 'File type not allowed'}), 400


@common_docs_bp.route('/api/common-documents/<document_id>', methods=['GET'])
def get_document(document_id):
    """Get a specific document metadata."""
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
    from database import get_db
    db = get_db()
    
    try:
        if not ObjectId.is_valid(document_id):
             return jsonify({'success': False, 'message': 'Invalid document ID'}), 400
             
        doc = db.common_documents.find_one({'_id': ObjectId(document_id)})
        
        if not doc:
            return jsonify({'success': False, 'message': 'Document not found'}), 404
            
        serialized_doc = {
            '_id': str(doc.get('_id')),
            'name': doc.get('name'),
            'type': doc.get('type'),
            'description': doc.get('description', ''),
            'file_name': doc.get('file_name'),
            'file_size': doc.get('file_size', 0),
            'created_at': doc.get('created_at').isoformat() if doc.get('created_at') else None,
            'updated_at': doc.get('updated_at').isoformat() if doc.get('updated_at') else None
        }
        
        return jsonify({
            'success': True,
            'document': serialized_doc
        })
        
    except Exception as e:
        logger.error(f"Error retrieving document: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@common_docs_bp.route('/api/common-documents/<document_id>', methods=['PUT'])
def update_document(document_id):
    """Update document metadata."""
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
    # Check permissions if needed
    
    from database import get_db
    db = get_db()
    
    try:
        data = request.get_json()
        
        update_data = {
            'updated_at': datetime.now()
        }
        
        if 'name' in data:
            update_data['name'] = data['name']
        if 'type' in data:
            update_data['type'] = data['type']
        if 'description' in data:
            update_data['description'] = data['description']
            
        # Note: File update not supported in PUT metadata, would need separate re-upload logic if desired
        
        result = db.common_documents.update_one(
            {'_id': ObjectId(document_id)},
            {'$set': update_data}
        )
        
        if result.matched_count == 0:
            return jsonify({'success': False, 'message': 'Document not found'}), 404
            
        return jsonify({
            'success': True,
            'message': 'Document updated successfully'
        })
        
    except Exception as e:
        logger.error(f"Error updating document: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@common_docs_bp.route('/api/common-documents/<document_id>', methods=['DELETE'])
def delete_document(document_id):
    """Delete a document."""
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
    # Check admin or permission
    
    from database import get_db
    db = get_db()
    
    try:
        doc = db.common_documents.find_one({'_id': ObjectId(document_id)})
        
        if not doc:
            return jsonify({'success': False, 'message': 'Document not found'}), 404
            
        # Delete file from filesystem
        file_path = doc.get('file_path')
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Could not delete physical file {file_path}: {e}")
        
        # Delete from DB
        db.common_documents.delete_one({'_id': ObjectId(document_id)})
        
        return jsonify({
            'success': True,
            'message': 'Document deleted successfully'
        })
        
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@common_docs_bp.route('/api/common-documents/<document_id>/download', methods=['GET'])
def download_document(document_id):
    """Download the document file.
    
    Supports multiple storage methods:
    1. File path on local filesystem
    2. Base64 encoded data stored in MongoDB (data, fileData, or content field)
    """
    if not is_authenticated():
        return jsonify({'success': False, 'error': 'Authentication required'}), 401
        
    from database import get_db
    import base64
    from utils.file_utils import get_mime_type
    db = get_db()
    
    try:
        doc = db.common_documents.find_one({'_id': ObjectId(document_id)})
        
        if not doc:
            logger.error(f"[COMMON_DOC_DOWNLOAD] Document not found: {document_id}")
            return jsonify({'success': False, 'message': 'Document not found'}), 404
        
        filename = doc.get('file_name', doc.get('name', 'document'))
        file_data = None
        
        # Method 1: Try file path on disk
        file_path = doc.get('file_path')
        if file_path and os.path.exists(file_path):
            logger.info(f"[COMMON_DOC_DOWNLOAD] Serving {filename} from file path: {file_path}")
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename
            )
        
        # Method 2: Try base64 data stored in MongoDB
        base64_data = doc.get('data') or doc.get('fileData') or doc.get('content') or doc.get('file_data')
        if base64_data:
            try:
                file_data = base64.b64decode(base64_data)
                logger.info(f"[COMMON_DOC_DOWNLOAD] Decoded {len(file_data)} bytes from base64 for {filename}")
            except Exception as e:
                logger.error(f"[COMMON_DOC_DOWNLOAD] Failed to decode base64 data for {document_id}: {e}")
        
        if not file_data:
            logger.error(f"[COMMON_DOC_DOWNLOAD] No file data available for document {document_id}")
            logger.error(f"[COMMON_DOC_DOWNLOAD] Document fields: {list(doc.keys())}")
            return jsonify({'success': False, 'message': 'File not found on server'}), 404
        
        # Determine MIME type
        mime_type = get_mime_type(filename)
        
        # Fallback if generic or unknown
        if not mime_type or mime_type == 'application/octet-stream':
            guessed_type, _ = mimetypes.guess_type(filename)
            if guessed_type:
                logger.info(f"[COMMON_DOC_DOWNLOAD] Guessed MIME type for {filename}: {guessed_type}")
                mime_type = guessed_type
        
        # Create response with proper headers
        response = make_response(file_data)
        response.headers['Content-Type'] = mime_type
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        response.headers['Content-Length'] = len(file_data)
        
        logger.info(f"[COMMON_DOC_DOWNLOAD] Successfully serving {filename} ({len(file_data)} bytes)")
        return response
        
    except Exception as e:
        logger.error(f"Error downloading document {document_id}: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

