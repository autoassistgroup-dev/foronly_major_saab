"""
File Handling Utilities

Provides file operation helpers for:
- File extension validation
- MIME type detection
- File type info with icons/colors
- File size formatting
- Persisting ticket attachments to disk (n8n / API)

Author: AutoAssistGroup Development Team
"""

import os
import re
import base64
import mimetypes
from datetime import datetime


# Allowed file extensions for uploads
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'jpg', 'jpeg', 'png', 'txt', 'csv'}


def allowed_file(filename):
    """
    Check if file extension is in allowed list.
    
    Args:
        filename: Name of the file to check
        
    Returns:
        bool: True if file extension is allowed
    """
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def format_file_size(size_bytes):
    """
    Format file size in human readable format.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Human readable file size string (e.g., "1.5 MB")
    """
    if not size_bytes:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    
    return f"{size_bytes:.1f} PB"


def get_mime_type(filename):
    """
    Get MIME type based on file extension.
    
    Args:
        filename: Name of the file
        
    Returns:
        MIME type string or 'application/octet-stream' as fallback
    """
    if not filename:
        return 'application/octet-stream'
    
    mime_type, _ = mimetypes.guess_type(filename)
    return mime_type or 'application/octet-stream'


def get_enhanced_file_type_info(filename, file_size=0):
    """
    Advanced file type detection with comprehensive MIME type mapping.
    Returns detailed file information including icons, colors, and capabilities.
    
    Args:
        filename: Name of the file
        file_size: Size of file in bytes (optional)
        
    Returns:
        dict: File information with icon, color, type, mime, viewable, category
    """
    extension = filename.split('.').pop().lower() if filename else ''
    
    file_type_mapping = {
        # Document types
        'pdf': {
            'icon': 'fas fa-file-pdf', 
            'color': 'text-red-600', 
            'type': 'PDF Document',
            'mime': 'application/pdf',
            'viewable': True,
            'category': 'document'
        },
        'doc': {
            'icon': 'fas fa-file-word', 
            'color': 'text-blue-600', 
            'type': 'Word Document',
            'mime': 'application/msword',
            'viewable': False,
            'category': 'document'
        },
        'docx': {
            'icon': 'fas fa-file-word', 
            'color': 'text-blue-600', 
            'type': 'Word Document',
            'mime': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'viewable': False,
            'category': 'document'
        },
        'xls': {
            'icon': 'fas fa-file-excel', 
            'color': 'text-green-600', 
            'type': 'Excel Spreadsheet',
            'mime': 'application/vnd.ms-excel',
            'viewable': False,
            'category': 'spreadsheet'
        },
        'xlsx': {
            'icon': 'fas fa-file-excel', 
            'color': 'text-green-600', 
            'type': 'Excel Spreadsheet',
            'mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'viewable': False,
            'category': 'spreadsheet'
        },
        'ppt': {
            'icon': 'fas fa-file-powerpoint', 
            'color': 'text-orange-600', 
            'type': 'PowerPoint Presentation',
            'mime': 'application/vnd.ms-powerpoint',
            'viewable': False,
            'category': 'presentation'
        },
        'pptx': {
            'icon': 'fas fa-file-powerpoint', 
            'color': 'text-orange-600', 
            'type': 'PowerPoint Presentation',
            'mime': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'viewable': False,
            'category': 'presentation'
        },
        # Image types
        'jpg': {
            'icon': 'fas fa-file-image', 
            'color': 'text-purple-600', 
            'type': 'JPEG Image',
            'mime': 'image/jpeg',
            'viewable': True,
            'category': 'image'
        },
        'jpeg': {
            'icon': 'fas fa-file-image', 
            'color': 'text-purple-600', 
            'type': 'JPEG Image',
            'mime': 'image/jpeg',
            'viewable': True,
            'category': 'image'
        },
        'png': {
            'icon': 'fas fa-file-image', 
            'color': 'text-purple-600', 
            'type': 'PNG Image',
            'mime': 'image/png',
            'viewable': True,
            'category': 'image'
        },
        'gif': {
            'icon': 'fas fa-file-image', 
            'color': 'text-purple-600', 
            'type': 'GIF Image',
            'mime': 'image/gif',
            'viewable': True,
            'category': 'image'
        },
        'webp': {
            'icon': 'fas fa-file-image', 
            'color': 'text-purple-600', 
            'type': 'WebP Image',
            'mime': 'image/webp',
            'viewable': True,
            'category': 'image'
        },
        # Archive types
        'zip': {
            'icon': 'fas fa-file-archive', 
            'color': 'text-yellow-600', 
            'type': 'ZIP Archive',
            'mime': 'application/zip',
            'viewable': False,
            'category': 'archive'
        },
        'rar': {
            'icon': 'fas fa-file-archive', 
            'color': 'text-yellow-600', 
            'type': 'RAR Archive',
            'mime': 'application/vnd.rar',
            'viewable': False,
            'category': 'archive'
        },
        '7z': {
            'icon': 'fas fa-file-archive', 
            'color': 'text-yellow-600', 
            'type': '7-Zip Archive',
            'mime': 'application/x-7z-compressed',
            'viewable': False,
            'category': 'archive'
        },
        # Text types
        'txt': {
            'icon': 'fas fa-file-alt', 
            'color': 'text-gray-600', 
            'type': 'Text File',
            'mime': 'text/plain',
            'viewable': True,
            'category': 'text'
        },
        'csv': {
            'icon': 'fas fa-file-csv', 
            'color': 'text-green-600', 
            'type': 'CSV File',
            'mime': 'text/csv',
            'viewable': True,
            'category': 'data'
        },
        'json': {
            'icon': 'fas fa-file-code', 
            'color': 'text-indigo-600', 
            'type': 'JSON File',
            'mime': 'application/json',
            'viewable': True,
            'category': 'data'
        },
        'xml': {
            'icon': 'fas fa-file-code', 
            'color': 'text-indigo-600', 
            'type': 'XML File',
            'mime': 'application/xml',
            'viewable': True,
            'category': 'data'
        }
    }
    
    file_info = file_type_mapping.get(extension, {
        'icon': 'fas fa-file', 
        'color': 'text-gray-600', 
        'type': 'File',
        'mime': 'application/octet-stream',
        'viewable': False,
        'category': 'unknown'
    })
    
    # Add file size information
    file_info['size'] = file_size
    file_info['size_formatted'] = format_file_size(file_size)
    file_info['extension'] = extension.upper()
    
    return file_info


def detect_warranty_form(filename, file_data=None):
    """
    Intelligent warranty form detection based on filename and content.
    Enhanced with comprehensive keyword matching.
    
    Args:
        filename: Name of the file
        file_data: Optional file content for content-based detection
        
    Returns:
        bool: True if file appears to be a warranty form
    """
    if not filename:
        return False
    
    # Comprehensive warranty keywords including common misspellings
    warranty_keywords = [
        'warranty', 'guarantee', 'warrantee', 'warrenty', 'guarante', 'garentee',
        'extended', 'protection', 'coverage', 'service_plan', 'service_contract',
        'maintenance_agreement', 'care_plan', 'support_plan', 'repair_coverage',
        'product_protection', 'extended_service', 'service_warranty', 
        'manufacturer_warranty', 'factory_warranty', 'vehicle_warranty',
        'bumper_to_bumper', 'powertrain', 'drivetrain', 'comprehensive_coverage',
        'dpf', 'diesel', 'emission', 'claim', 'form', 'customer',
        'repair', 'service', 'defect', 'malfunction', 'issue', 'fault',
        'warranty_form', 'warranty_claim', 'claim_form', 'service_form'
    ]
    
    filename_lower = filename.lower()
    
    # Check filename for warranty keywords
    for keyword in warranty_keywords:
        if keyword in filename_lower:
            return True
    
    # Future enhancement: Content-based analysis
    if file_data:
        # Framework for content analysis (OCR, text extraction, etc.)
        pass
    
    return False


def safe_attachment_filename(name, max_len=200):
    """
    Sanitize attachment filename for filesystem: no path traversal, no nulls.
    """
    if not name or not isinstance(name, str):
        return "attachment"
    # Remove path components and null bytes
    name = os.path.basename(name).replace("\x00", "").strip()
    # Allow only alphanumeric, dash, underscore, dot
    name = re.sub(r"[^\w\-.]", "_", name, flags=re.ASCII)
    return name[:max_len] if name else "attachment"


def extract_attachment_bytes(att):
    """
    Extract raw bytes from an attachment dict (n8n / API payload).
    Tries: data, fileData, content, binary.data (base64).
    Returns (bytes or None, error_message or None).
    """
    if not att or not isinstance(att, dict):
        return None, "attachment not a dict"
    raw = att.get("data") or att.get("fileData") or att.get("content")
    if raw is None and isinstance(att.get("binary"), dict):
        raw = att["binary"].get("data")
    if raw is None:
        return None, "no data/fileData/content/binary.data"
    if isinstance(raw, bytes):
        return raw, None
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw, validate=True), None
        except Exception as e:
            return None, str(e)
    return None, "unsupported data type"


def save_ticket_attachment_to_disk(ticket_id, attachment_dict, index, upload_root):
    """
    Persist one ticket attachment to disk and return metadata dict for MongoDB.
    Writes under upload_root/tickets/<ticket_id>/.
    Accepts attachment dict with base64 in data/fileData/content/binary.data.
    Returns dict with: filename, file_path, data (base64), mime_type, size, uploaded_at.
    IMPORTANT: We now also store the base64 'data' in MongoDB so that Vercel
    (serverless / ephemeral disk) can still serve attachments after deploy.
    On failure returns None and the caller can keep original or drop.
    """
    if not attachment_dict or not isinstance(attachment_dict, dict):
        return None
    fn = (
        attachment_dict.get("filename")
        or attachment_dict.get("fileName")
        or attachment_dict.get("name")
        or "attachment"
    )
    fn = safe_attachment_filename(fn)
    data_bytes, err = extract_attachment_bytes(attachment_dict)
    if data_bytes is None:
        return None
    try:
        ticket_dir = os.path.join(upload_root, "tickets", str(ticket_id))
        os.makedirs(ticket_dir, exist_ok=True)
        ts = int(datetime.now().timestamp())
        base_name, ext = os.path.splitext(fn)
        if not ext and len(base_name) > 32:
            base_name = base_name[:32]
        unique_name = f"{base_name}_{index}_{ts}{ext}"
        file_path = os.path.join(ticket_dir, unique_name)
        with open(file_path, "wb") as f:
            f.write(data_bytes)
        size = len(data_bytes)
        mime_type = get_mime_type(fn)
        # Store base64 data in MongoDB so Vercel ephemeral disk doesn't break previews
        b64_data = base64.b64encode(data_bytes).decode('utf-8')
        return {
            "filename": fn,
            "fileName": fn,
            "file_path": file_path,
            "data": b64_data,
            "mime_type": mime_type,
            "size": size,
            "uploaded_at": datetime.now(),
        }
    except Exception:
        return None


def save_attachment_bytes_to_disk(upload_root, subdir, unique_prefix, filename, data_bytes):
    """
    Save raw bytes to disk under upload_root/subdir/ with a unique name.
    Used for claim docs, reply attachments, and UI ticket attachments.
    Returns dict with file_path, filename, data (base64), mime_type, size or None on failure.
    Also stores base64 data so Vercel ephemeral disk doesn't break previews.
    """
    if not data_bytes or not filename:
        return None
    fn = safe_attachment_filename(filename)
    try:
        dir_path = os.path.join(upload_root, subdir)
        os.makedirs(dir_path, exist_ok=True)
        ts = int(datetime.now().timestamp())
        base_name, ext = os.path.splitext(fn)
        if not ext and len(base_name) > 32:
            base_name = base_name[:32]
        unique_name = f"{unique_prefix}_{ts}_{base_name}{ext}"
        file_path = os.path.join(dir_path, unique_name)
        with open(file_path, "wb") as f:
            f.write(data_bytes)
        b64_data = base64.b64encode(data_bytes).decode('utf-8')
        return {
            "filename": fn,
            "file_path": file_path,
            "data": b64_data,
            "mime_type": get_mime_type(fn),
            "size": len(data_bytes),
        }
    except Exception:
        return None

def get_attachment_signature(att):
    """
    Generate a quick, unique signature for an attachment to detect duplicates.
    It hashes the first 10,000 chars of the base64 data (or bytes).
    If no data is present, it hashes the filename and size.
    """
    import hashlib
    if not att or not isinstance(att, dict):
        return "invalid"
        
    data, _ = extract_attachment_bytes(att)
    if data:
        # Hash the first 10KB of raw bytes for speed, usually enough to prove uniqueness
        return hashlib.md5(data[:10000]).hexdigest()
        
    filename = att.get('filename', att.get('fileName', att.get('name', '')))
    size = att.get('size', 0)
    return hashlib.md5(f"{filename}_{size}".encode('utf-8')).hexdigest()
