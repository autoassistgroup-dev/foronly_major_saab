"""
MongoDB Database Layer for AutoAssistGroup Support System

This module provides a comprehensive MongoDB interface for the support ticket system,
featuring optimized serverless connections, comprehensive error handling, and
efficient query patterns for high-performance ticket management.

Key Features:
- Serverless-optimized connection management
- Comprehensive error handling and logging
- Efficient aggregation pipelines for ticket assignment
- Warranty detection and analytics
- Role-based access control
- Index optimization for fast queries

Author: AutoAssistGroup Development Team
"""

import os
import pymongo
import base64
import time
import threading
from pymongo import MongoClient
from datetime import datetime
from werkzeug.security import generate_password_hash
import uuid
import logging

# Reduce PyMongo logging verbosity
logging.getLogger('pymongo').setLevel(logging.WARNING)

# Sanitize logs for Windows consoles that cannot render emojis (cp1252)
class _AsciiLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
            sanitized = msg.encode('ascii', errors='ignore').decode('ascii', errors='ignore')
            record.msg = sanitized
            record.args = None
        except Exception:
            pass
        return True

try:
    _ascii_filter = _AsciiLogFilter()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(_ascii_filter)
except Exception:
    pass

class MongoDB:
    # ====== IN-MEMORY CACHE (shared across requests) ======
    _cache = {}
    _cache_lock = threading.Lock()
    _CACHE_TTL = {
        'system_settings': 300,   # 5 minutes
        'all_members': 120,       # 2 minutes
        'all_technicians': 120,   # 2 minutes
        'ticket_statuses': 300,   # 5 minutes
        'member': 60,             # 1 minute per member
    }

    @classmethod
    def _cache_get(cls, key):
        """Get a cached value if it exists and hasn't expired."""
        with cls._cache_lock:
            entry = cls._cache.get(key)
            if entry and time.time() - entry['ts'] < entry['ttl']:
                return entry['value']
            # Expired or missing
            if entry:
                del cls._cache[key]
            return None

    @classmethod
    def _cache_set(cls, key, value, ttl=60):
        """Cache a value with TTL in seconds."""
        with cls._cache_lock:
            cls._cache[key] = {'value': value, 'ts': time.time(), 'ttl': ttl}

    @classmethod
    def invalidate_cache(cls, key=None):
        """Clear a specific cache key or all caches."""
        with cls._cache_lock:
            if key:
                cls._cache.pop(key, None)
            else:
                cls._cache.clear()

    def __init__(self):
        # MongoDB connection with optimized serverless configuration
        self.connection_string = os.environ.get('MONGODB_URI')
        
        if not self.connection_string:
            raise ValueError("MONGODB_URI environment variable is required")
        
        # Serverless-optimized connection settings
        try:
            # Try to use certifi for SSL certificates (recommended)
            import ssl
            try:
                import certifi
                tls_ca_file = certifi.where()
                logging.info(f"Using certifi CA bundle: {tls_ca_file}")
            except ImportError:
                tls_ca_file = None
                logging.warning("certifi not installed, SSL certificate verification may fail")
            
            connection_options = {
                # PERFORMANCE: Serverless optimized connection pool
                'maxPoolSize': 10,
                'minPoolSize': 0,                     # MUST be 0 for Serverless to prevent zombie connections
                'maxIdleTimeMS': 10000,               # Aggressive pool cleanup (10s)
                'serverSelectionTimeoutMS': 5000,     # Fast-fail on DNS issues
                'connectTimeoutMS': 10000,            # 10s network connect
                'socketTimeoutMS': 45000,             # 45s socket limit (let Vercel govern overall timeout)
                'heartbeatFrequencyMS': 30000,        # 30 seconds
                'retryWrites': True,
                'retryReads': True,
                'w': 'majority',
                'readPreference': 'primaryPreferred',
                'maxConnecting': 2
            }
            
            # Add SSL certificate handling
            if tls_ca_file:
                connection_options['tlsCAFile'] = tls_ca_file
            else:
                # For development on macOS without certifi - disable cert verification
                # WARNING: Not recommended for production!
                connection_options['tlsAllowInvalidCertificates'] = True
                logging.warning("SSL certificate verification disabled - install certifi for production use")
            
            self.client = MongoClient(
                self.connection_string,
                **connection_options
            )
            
            # Test the connection immediately
            self.client.admin.command('ping')
            self.db = self.client.support_tickets
            
            # Collections
            self.tickets = self.db.tickets
            self.replies = self.db.replies
            self.members = self.db.members
            self.ticket_assignments = self.db.ticket_assignments
            self.ticket_metadata = self.db.ticket_metadata
            self.technicians = self.db.technicians  # Standalone technician collection
            self.ticket_statuses = self.db.ticket_statuses  # Ticket status management
            self.roles = self.db.roles  # Role management collection
            self.common_documents = self.db.common_documents  # Common documents collection
            self.common_document_metadata = self.db.common_document_metadata  # 🚀 NEW: Common document metadata collection
            self.claim_documents = self.db.claim_documents  # Claim documents collection (receipts, photos, etc.)
            
            # Initialize database with indexes and admin user
            self.init_database()
            
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise
    
    def init_database(self):
        """Initialize database with indexes and default admin user"""
        try:
            # Create indexes for better performance (with background=False for faster startup)
            try:
                self.tickets.create_index("ticket_id", unique=True, background=False)
            except pymongo.errors.DuplicateKeyError:
                pass
                
            try:
                self.tickets.create_index("thread_id", unique=True, background=False)
            except pymongo.errors.DuplicateKeyError:
                pass
                
            try:
                self.members.create_index("user_id", unique=True, background=False)
            except pymongo.errors.DuplicateKeyError:
                pass
            
            # Additional indexes (non-unique)
            self.tickets.create_index([("email", 1), ("status", 1)], background=False)
            self.tickets.create_index([("created_at", -1)], background=False)
            
            # Common documents indexes
            try:
                self.common_documents.create_index([("name", 1)], background=False)
                self.common_documents.create_index([("type", 1)], background=False)
                self.common_documents.create_index([("created_at", -1)], background=False)
            except Exception as e:
                logging.warning(f"Could not create common documents indexes: {e}")
            
            # Common document metadata indexes
            try:
                self.common_document_metadata.create_index([("document_id", 1), ("key", 1)], background=False)
                self.common_document_metadata.create_index([("document_id", 1)], background=False)
            except Exception as e:
                logging.warning(f"Could not create common document metadata indexes: {e}")
            
            # Claim documents indexes
            try:
                self.claim_documents.create_index([("ticket_id", 1)], background=False)
                self.claim_documents.create_index([("ticket_id", 1), ("is_deleted", 1)], background=False)
                self.claim_documents.create_index([("uploaded_at", -1)], background=False)
            except Exception as e:
                logging.warning(f"Could not create claim documents indexes: {e}")
                
            self.tickets.create_index([("status", 1), ("priority", 1)], background=False)
            self.replies.create_index([("ticket_id", 1), ("created_at", 1)], background=False)
            self.ticket_assignments.create_index([("ticket_id", 1), ("member_id", 1)], background=False)
            self.ticket_metadata.create_index([("ticket_id", 1), ("key", 1)], background=False)
            
            # CRITICAL PERFORMANCE INDEX: Support dashboard default sort
            # Pure chronological: newest activity on top
            self.tickets.create_index(
                [("updated_at", -1)],
                background=False
            )
            
            # Additional unread index for quick lookups
            self.tickets.create_index([("has_unread_reply", 1)], background=False)
            
            # Enhanced indexes for warranty detection and attachment support
            self.tickets.create_index([("has_warranty", 1)], background=False)
            self.tickets.create_index([("has_attachments", 1)], background=False)
            self.tickets.create_index([("warranty_forms_count", 1)], background=False)
            self.tickets.create_index([("total_attachments", 1)], background=False)
            self.tickets.create_index([("processing_method", 1)], background=False)
            self.tickets.create_index([("has_warranty", 1), ("created_at", -1)], background=False)
            self.tickets.create_index([("has_attachments", 1), ("status", 1)], background=False)
            
            # Create admin user if it doesn't exist
            admin_exists = self.members.find_one({"user_id": "admin001"})
            if not admin_exists:
                admin_user = {
                    "name": "Admin",
                    "role": "Administrator", 
                    "gender": "male",
                    "user_id": "admin001",
                    "password_hash": generate_password_hash("admin@123"),
                    "created_at": datetime.now()
                }
                self.members.insert_one(admin_user)
                logging.info("Admin user created successfully")
            
            # Create Technical Director user if it doesn't exist
            tech_director_exists = self.members.find_one({"user_id": "marc001"})
            if not tech_director_exists:
                tech_director_user = {
                    "name": "Marc (Technical Director)",
                    "role": "Technical Director", 
                    "gender": "male",
                    "user_id": "marc001",
                    "password_hash": generate_password_hash("tech@123"),
                    "created_at": datetime.now(),
                    "email": "marc@autoassistgroup.com",
                    "department": "Technical",
                    "is_active": True
                }
                self.members.insert_one(tech_director_user)
                logging.info("Technical Director user created successfully")
            
            # NOTE: Do not auto-seed IT Support members. Admin will create them via the panel.
            
            # Create initial technicians if they don't exist
            if self.technicians.count_documents({}) == 0:
                initial_technicians = [
                    {"name": "Ryan", "role": "Senior Technician", "email": "ryan@autoassistgroup.com"},
                    {"name": "Declan", "role": "Technician", "email": "declan@autoassistgroup.com"},
                    {"name": "Ross H", "role": "Lead Technician", "email": "ross.h@autoassistgroup.com"},
                    {"name": "Ross K", "role": "Technician", "email": "ross.k@autoassistgroup.com"},
                    {"name": "Ray", "role": "Senior Technician", "email": "ray@autoassistgroup.com"},
                    {"name": "Craig", "role": "Technician", "email": "craig@autoassistgroup.com"},
                    {"name": "Karl", "role": "Lead Technician", "email": "karl@autoassistgroup.com"},
                    {"name": "Matthew", "role": "Technician", "email": "matthew@autoassistgroup.com"},
                    {"name": "Lewis", "role": "Senior Technician", "email": "lewis@autoassistgroup.com"}
                ]
                for tech_data in initial_technicians:
                    technician_data = {
                        "name": tech_data["name"],
                        "role": tech_data["role"],
                        "email": tech_data["email"],
                        "is_active": True,
                        "created_at": datetime.now()
                    }
                    self.technicians.insert_one(technician_data)
                logging.info(f"Created {len(initial_technicians)} initial technicians")

            # Initialize default ticket statuses
            self.initialize_default_statuses()
            
            # Initialize default roles
            self.initialize_default_roles()
                
        except pymongo.errors.DuplicateKeyError:
            # Index already exists, ignore
            pass
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Database operation failed during initialization: {e}")
        except Exception as e:
            logging.error(f"Database initialization error: {e}")
    
    def migrate_has_unread_reply_field(self):
        """Migrate existing tickets to ensure they all have the has_unread_reply field"""
        try:
            logging.info("[DATABASE] Starting migration of has_unread_reply field for existing tickets...")
            
            # Find all tickets that don't have the has_unread_reply field
            tickets_missing_field = list(self.tickets.find(
                {"has_unread_reply": {"$exists": False}},
                {"ticket_id": 1, "_id": 1}
            ))
            
            if not tickets_missing_field:
                logging.info("[DATABASE] All tickets already have has_unread_reply field - no migration needed")
                return True
            
            logging.info(f"[DATABASE] Found {len(tickets_missing_field)} tickets missing has_unread_reply field")
            
            # Update all missing tickets to have has_unread_reply = False
            result = self.tickets.update_many(
                {"has_unread_reply": {"$exists": False}},
                {"$set": {"has_unread_reply": False}}
            )
            
            logging.info(f"[DATABASE] Migration complete: Updated {result.modified_count} tickets with has_unread_reply=False")
            
            # Verify the migration
            remaining_missing = self.tickets.count_documents({"has_unread_reply": {"$exists": False}})
            if remaining_missing == 0:
                logging.info("[DATABASE] Migration verification successful: All tickets now have has_unread_reply field")
                return True
            else:
                logging.error(f"[DATABASE] Migration verification failed: {remaining_missing} tickets still missing has_unread_reply field")
                return False
                
        except Exception as e:
            logging.error(f"[DATABASE] Error during has_unread_reply migration: {e}")
            return False

    def get_tickets_with_assignments(self, page=1, per_page=20, status_filter=None, priority_filter=None, search_query=None, referred_only=False, exclude_ids=None):
        """Get tickets with assignment information and technician data - OPTIMIZED PAGINATED VERSION"""
        try:
            # Build match stage for filtering
            match_stage = {}
            
            if referred_only:
                match_stage["status"] = {"$regex": "Referred", "$options": "i"}
            
            if exclude_ids:
                match_stage["ticket_id"] = {"$nin": exclude_ids}
                
            if status_filter and status_filter != 'All':
                match_stage["status"] = status_filter
                
            if priority_filter and priority_filter != 'All':
                match_stage["priority"] = priority_filter
            if search_query:
                match_stage["$or"] = [
                    {"ticket_id": {"$regex": search_query, "$options": "i"}},
                    {"subject": {"$regex": search_query, "$options": "i"}},
                    {"name": {"$regex": search_query, "$options": "i"}},
                    {"email": {"$regex": search_query, "$options": "i"}}
                ]
            
            pipeline = []
            
            if match_stage:
                pipeline.append({"$match": match_stage})
            
            # Sort -> Skip -> Limit BEFORE lookups and computed fields (MASSIVE performance fix for timeouts)
            # Sort by native indexed _id first which represents creation date
            pipeline.append({"$sort": {"_id": -1}})
            
            skip = (page - 1) * per_page
            pipeline.extend([
                {"$skip": skip},
                {"$limit": per_page}
            ])
            
            # Post-pagination calculated fields (operates only on the 20-50 returned docs)
            pipeline.append({"$addFields": {
                "updated_at": {"$ifNull": ["$updated_at", "$created_at"]},
                "has_unread_notification": {"$ifNull": ["$has_unread_notification", False]}
            }})
            
            # Lookups only on paginated results
            pipeline.extend([
                {
                    "$lookup": {
                        "from": "ticket_assignments",
                        "localField": "ticket_id",
                        "foreignField": "ticket_id",
                        "as": "assignment"
                    }
                },
                {
                    "$addFields": {
                        "assignment_member_id": {"$arrayElemAt": ["$assignment.member_id", 0]},
                        "assignment_forwarded_from": {"$arrayElemAt": ["$assignment.forwarded_from", 0]}
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "assignment_member_id",
                        "foreignField": "_id",
                        "as": "assigned_member"
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "assignment_forwarded_from",
                        "foreignField": "_id",
                        "as": "forwarded_from_member"
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_to",
                        "foreignField": "_id",
                        "as": "forwarded_to_member"
                    }
                },
                # PERFORMANCE FIX: Technician metadata is now included via $lookup
                # instead of N+1 individual queries per ticket
                {
                    "$lookup": {
                        "from": "ticket_metadata",
                        "localField": "ticket_id",
                        "foreignField": "ticket_id",
                        "as": "_tech_metadata"
                    }
                },
                # Extract technician info from metadata and ensure has_unread_reply
                {
                    "$addFields": {
                        "has_unread_reply": {"$ifNull": ["$has_unread_reply", False]},
                        "technician_id": {
                            "$let": {
                                "vars": {
                                    "tech_id_doc": {
                                        "$arrayElemAt": [
                                            {"$filter": {
                                                "input": {"$ifNull": ["$_tech_metadata", []]},
                                                "cond": {"$eq": ["$$this.key", "technician_id"]}
                                            }}, 0
                                        ]
                                    }
                                },
                                "in": {"$ifNull": ["$$tech_id_doc.value", None]}
                            }
                        },
                        "technician_name": {
                            "$let": {
                                "vars": {
                                    "tech_name_doc": {
                                        "$arrayElemAt": [
                                            {"$filter": {
                                                "input": {"$ifNull": ["$_tech_metadata", []]},
                                                "cond": {"$eq": ["$$this.key", "technician_name"]}
                                            }}, 0
                                        ]
                                    }
                                },
                                "in": {"$ifNull": ["$$tech_name_doc.value", None]}
                            }
                        }
                    }
                },
                # Remove temporary fields
                {
                    "$project": {
                        "assignment_member_id": 0,
                        "assignment_forwarded_from": 0,
                        "_tech_metadata": 0
                    }
                }
            ])
            
            result = list(self.tickets.aggregate(pipeline))
            
            # Ensure has_unread_reply and has_unread_notification are boolean on all tickets
            for ticket in result:
                if 'has_unread_reply' not in ticket or not isinstance(ticket.get('has_unread_reply'), bool):
                    ticket['has_unread_reply'] = bool(ticket.get('has_unread_reply', False))
                if 'has_unread_notification' not in ticket or not isinstance(ticket.get('has_unread_notification'), bool):
                    ticket['has_unread_notification'] = bool(ticket.get('has_unread_notification', False))
            
            return result
            
        except pymongo.errors.OperationFailure as e:
            self.last_error = f"[DATABASE] Failed to get tickets (OperationFailure): {str(e)}\n{traceback.format_exc()}"
            logging.error(self.last_error)
            return []
        except Exception as e:
            import traceback
            self.last_error = f"[DATABASE] Error getting tickets (Exception): {str(e)}\n{traceback.format_exc()}"
            logging.error(self.last_error)
            return []
    
    def get_tickets_count(self, status_filter=None, priority_filter=None, search_query=None, referred_only=False, exclude_ids=None):
        """Get total count of tickets for pagination"""
        try:
            # Build match stage for filtering (same as get_tickets_with_assignments)
            match_stage = {}
            
            # Technical Director filtering - only count referred tickets
            if referred_only:
                match_stage["status"] = {"$regex": "Referred", "$options": "i"}
                
            # Exclude specific ticket IDs
            if exclude_ids:
                match_stage["ticket_id"] = {"$nin": exclude_ids}
                
            if status_filter and status_filter != 'All':
                match_stage["status"] = status_filter
                
            if priority_filter and priority_filter != 'All':
                match_stage["priority"] = priority_filter
            if search_query:
                match_stage["$or"] = [
                    {"ticket_id": {"$regex": search_query, "$options": "i"}},
                    {"subject": {"$regex": search_query, "$options": "i"}},
                    {"name": {"$regex": search_query, "$options": "i"}},
                    {"email": {"$regex": search_query, "$options": "i"}}
                ]
            
            # Count documents with the same filters
            count = self.tickets.count_documents(match_stage)
            return count
            
        except Exception as e:
            logging.error(f"[DATABASE] Error getting tickets count: {e}")
            return 0

    def get_ticket_stats(self):
        """
        Get efficient aggregated statistics for tickets.
        Performance Optimized: Uses server-side aggregation instead of loading all docs.
        """
        try:
            pipeline = [
                {
                    "$facet": {
                        "status_counts": [
                            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
                        ],
                        "priority_counts": [
                             {"$group": {"_id": "$priority", "count": {"$sum": 1}}}
                        ],
                        "classification_counts": [
                             {"$group": {"_id": "$classification", "count": {"$sum": 1}}}
                        ],
                        "total_count": [
                            {"$count": "count"}
                        ]
                    }
                }
            ]
            
            result = list(self.tickets.aggregate(pipeline))
            
            if not result:
                return {}
                
            stats = result[0]
            
            # Format output for easier consumption
            formatted_stats = {
                "status_counts": {item["_id"]: item["count"] for item in stats.get("status_counts", [])},
                "priorities": {item["_id"]: item["count"] for item in stats.get("priority_counts", [])},
                "classifications": {item["_id"]: item["count"] for item in stats.get("classification_counts", [])},
                "total_tickets": stats.get("total_count", [{"count": 0}])[0]["count"] if stats.get("total_count") else 0
            }
            
            # Fill in defaults if missing
            default_priorities = {'Urgent': 0, 'Fast': 0, 'High': 0, 'Medium': 0, 'Low': 0}
            for p, count in formatted_stats["priorities"].items():
                if p in default_priorities:
                    default_priorities[p] = count
            formatted_stats["priorities"] = default_priorities
            
            # Fill in default classifications if missing (required for UI)
            default_classifications = {'Technical Issue': 0, 'Payment': 0, 'Support': 0, 'Warranty Claim': 0, 'Spam': 0, 'Account': 0}
            for c, count in formatted_stats["classifications"].items():
                if c in default_classifications:
                    default_classifications[c] = count
            formatted_stats["classifications"] = default_classifications
            
            return formatted_stats
            
        except Exception as e:
            logging.error(f"[DATABASE] Error getting ticket stats: {e}")
            return {
                "status_counts": {}, 
                "priorities": {'Urgent': 0, 'Fast': 0, 'High': 0, 'Medium': 0, 'Low': 0},
                "classifications": {},
                "total_tickets": 0
            }

    def set_ticket_unread(self, ticket_id, state=True):
        """Set the unread notification flag for a ticket"""
        try:
            self.tickets.update_one(
                {"ticket_id": ticket_id},
                {"$set": {"has_unread_notification": state}}
            )
            return True
        except Exception as e:
            logging.error(f"Failed to set ticket unread for {ticket_id}: {e}")
            return False

    def mark_ticket_viewed(self, ticket_id):
        """Clear the unread notification and reply flags for a ticket"""
        try:
            self.tickets.update_one(
                {"ticket_id": ticket_id},
                {"$set": {
                    "has_unread_notification": False,
                    "has_unread_reply": False
                }}
            )
            return True
        except Exception as e:
            logging.error(f"Failed to mark ticket viewed for {ticket_id}: {e}")
            return False

    def get_forwarded_tickets_to_user(self, member_id):
        """
        Get all tickets that have been forwarded TO a specific user.
        
        Returns tickets with full forwarding metadata including:
        - forwarded_from member info (name, role)
        - forwarded_at timestamp
        - is_forwarded_viewed status
        
        Args:
            member_id: The member ID to check forwarded tickets for
            
        Returns:
            List of ticket documents with forwarding info
        """
        try:
            from bson.objectid import ObjectId
            
            # Support both ObjectId and string in DB (forwarded_to may be stored as either)
            member_id_str = str(member_id) if member_id is not None else ''
            member_id_obj = None
            if member_id is not None:
                try:
                    member_id_obj = ObjectId(member_id) if isinstance(member_id, str) else member_id
                except Exception:
                    member_id_obj = member_id
            
            # Match forwarded_to as either ObjectId or string so we find tickets regardless of storage format
            match_values = [v for v in [member_id_obj, member_id_str] if v is not None and v != '']
            forwarded_to_criteria = {"$in": match_values} if len(match_values) > 1 else (match_values[0] if match_values else None)
            if forwarded_to_criteria is None:
                logging.warning("[DATABASE] get_forwarded_tickets_to_user: invalid member_id, returning []")
                return []
            

            # Use $or to match both ObjectId and string formats explicitly
            match_conditions = []
            if member_id_obj:
                match_conditions.append({"forwarded_to": member_id_obj})
            if member_id_str:
                match_conditions.append({"forwarded_to": member_id_str})
            
            if not match_conditions:
                logging.warning("[DATABASE] No valid match conditions for forwarded_to")
                return []
            
            pipeline = [
                # Match tickets that are forwarded TO this user (ObjectId OR string)
                {
                    "$match": {
                        "is_forwarded": True,
                        "$or": match_conditions
                    }
                },
                # Sort by forwarded_at (newest first), then by is_forwarded_viewed (unviewed first)
                {
                    "$sort": {
                        "is_forwarded_viewed": 1,  # Unviewed (false) first
                        "forwarded_at": -1
                    }
                },
                # Lookup the member who forwarded the ticket
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_by",
                        "foreignField": "_id",
                        "as": "forwarded_from_member"
                    }
                },
                # Lookup the current user's (forwarded_to) member info
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_to",
                        "foreignField": "_id",
                        "as": "forwarded_to_member"
                    }
                },
                # Also get assignment info for this ticket
                {
                    "$lookup": {
                        "from": "ticket_assignments",
                        "localField": "ticket_id",
                        "foreignField": "ticket_id",
                        "as": "assignment"
                    }
                },
                # Add formatted date field and ensure is_forwarded_viewed has default
            # NOTE: MongoDB's $dateToString does NOT support %I (12-hour with leading zero)
            # Using %H:%M (24-hour format) instead
            {
                "$addFields": {
                    "is_forwarded_viewed": {"$ifNull": ["$is_forwarded_viewed", False]},
                    "formatted_forwarded_at": {
                        "$dateToString": {
                            "format": "%b %d, %H:%M",
                            "date": "$forwarded_at",
                            "timezone": "Europe/London"
                        }
                    },
                    "formatted_date": {
                        "$dateToString": {
                            "format": "%b %d, %H:%M",
                            "date": "$created_at",
                            "timezone": "Europe/London"
                        }
                    }
                }
            }
            ]
            
            result = list(self.tickets.aggregate(pipeline, allowDiskUse=True))
            
            # Format the forwarded_from info for easier template access
            for ticket in result:
                # Extract first member from lookup results
                if ticket.get('forwarded_from_member') and len(ticket['forwarded_from_member']) > 0:
                    from_member = ticket['forwarded_from_member'][0]
                    ticket['forwarded_from_name'] = from_member.get('name', 'Unknown')
                    ticket['forwarded_from_role'] = from_member.get('role', 'Member')
                else:
                    ticket['forwarded_from_name'] = 'Unknown'
                    ticket['forwarded_from_role'] = 'Member'
                
                if ticket.get('forwarded_to_member') and len(ticket['forwarded_to_member']) > 0:
                    to_member = ticket['forwarded_to_member'][0]
                    ticket['forwarded_to_name'] = to_member.get('name', 'You')
                else:
                    ticket['forwarded_to_name'] = 'You'
            
            return result
            
        except Exception as e:
            logging.error(f"[DATABASE] Error getting forwarded tickets: {e}")
            return []

    def get_forwarded_tickets_by_user(self, member_id):
        """
        Get tickets that were forwarded BY a specific user that are still pending/unactioned.
        
        A forwarded ticket is considered "actioned" (and hidden) when:
        - Its status is Resolved or Closed
        - Or is_forwarded has been cleared (ticket was un-forwarded)
        
        Args:
            member_id: The member who performed the forward
            
        Returns:
            List of pending forwarded ticket documents
        """
        try:
            from bson.objectid import ObjectId
            
            member_id_str = str(member_id) if member_id is not None else ''
            member_id_obj = None
            if member_id is not None:
                try:
                    member_id_obj = ObjectId(member_id) if isinstance(member_id, str) else member_id
                except Exception:
                    member_id_obj = member_id
            
            # Match both ObjectId and string formats for forwarded_by
            match_conditions = []
            if member_id_obj:
                match_conditions.append({"forwarded_by": member_id_obj})
            if member_id_str:
                match_conditions.append({"forwarded_by": member_id_str})
            
            if not match_conditions:
                return []
            
            pipeline = [
                {
                    "$match": {
                        "is_forwarded": True,
                        "$or": match_conditions,
                        # Only show unactioned: exclude Resolved/Closed
                        "status": {"$nin": ["Resolved", "Closed"]}
                    }
                },
                {"$sort": {"forwarded_at": -1}},
                # Lookup the person it was forwarded TO
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_to",
                        "foreignField": "_id",
                        "as": "forwarded_to_member"
                    }
                },
                # Lookup the forwarder (current user)
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_by",
                        "foreignField": "_id",
                        "as": "forwarded_from_member"
                    }
                },
                {
                    "$addFields": {
                        "formatted_forwarded_at": {
                            "$dateToString": {
                                "format": "%b %d, %H:%M",
                                "date": "$forwarded_at",
                                "timezone": "Europe/London"
                            }
                        },
                        "formatted_date": {
                            "$dateToString": {
                                "format": "%b %d, %H:%M",
                                "date": "$created_at",
                                "timezone": "Europe/London"
                            }
                        }
                    }
                }
            ]
            
            result = list(self.tickets.aggregate(pipeline, allowDiskUse=True))
            
            for ticket in result:
                if ticket.get('forwarded_to_member') and len(ticket['forwarded_to_member']) > 0:
                    to_member = ticket['forwarded_to_member'][0]
                    ticket['forwarded_to_name'] = to_member.get('name', 'Unknown')
                else:
                    ticket['forwarded_to_name'] = 'Unknown'
                
                if ticket.get('forwarded_from_member') and len(ticket['forwarded_from_member']) > 0:
                    from_member = ticket['forwarded_from_member'][0]
                    ticket['forwarded_from_name'] = from_member.get('name', 'You')
                    ticket['forwarded_from_role'] = from_member.get('role', 'Member')
                else:
                    ticket['forwarded_from_name'] = 'You'
                    ticket['forwarded_from_role'] = 'Member'
            
            return result
            
        except Exception as e:
            logging.error(f"[DATABASE] Error getting forwarded-by tickets: {e}")
            return []

    def mark_forwarded_ticket_viewed(self, ticket_id, member_id):
        """
        Mark a forwarded ticket as viewed by the forwarded-to member.
        
        Args:
            ticket_id: The ticket ID to mark as viewed
            member_id: The member viewing the ticket
            
        Returns:
            bool: True if updated successfully
        """
        try:
            from bson.objectid import ObjectId
            
            # Convert member_id to ObjectId if necessary
            if isinstance(member_id, str):
                try:
                    member_id_obj = ObjectId(member_id)
                except:
                    member_id_obj = member_id
            else:
                member_id_obj = member_id
            
            # Only mark as viewed if this ticket is forwarded TO this member
            result = self.tickets.update_one(
                {
                    "ticket_id": ticket_id,
                    "is_forwarded": True,
                    "forwarded_to": member_id_obj
                },
                {
                    "$set": {
                        "is_forwarded_viewed": True,
                        "forwarded_viewed_at": datetime.now(),
                        "has_unread_notification": False
                    }
                }
            )
            
            if result.modified_count > 0:
                logging.info(f"[DATABASE] Marked forwarded ticket {ticket_id} as viewed by member {member_id}")
                return True
            return False
            
        except Exception as e:
            logging.error(f"[DATABASE] Error marking forwarded ticket as viewed: {e}")
            return False

    def get_dashboard_stats(self):
        """
        Get specialized stats for the dashboard.
        """
        try:
            now = datetime.now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            three_days_ago = now - timedelta(days=3)
            
            pipeline = [
                {
                    "$facet": {
                        "overdue": [
                            {"$match": {
                                "created_at": {"$lt": three_days_ago},
                                "status": {"$nin": ["Resolved", "Closed"]}
                            }},
                             # Limit to save memory if list is needed, or just count
                            {"$limit": 50} 
                        ],
                        "unread": [
                            {"$match": {"has_unread_reply": True}}
                        ],
                        "claims_stats": [
                             {"$group": {
                                "_id": None,
                                "total": {"$sum": 1},
                                "approved": {
                                    "$sum": {
                                        "$cond": [{"$regexMatch": {"input": "$status", "regex": "Approved|Revisit"}}, 1, 0]
                                    }
                                },
                                "declined": {
                                    "$sum": {
                                        "$cond": [{"$regexMatch": {"input": "$status", "regex": "Declined|Not Covered"}}, 1, 0]
                                    }
                                },
                                "referred": {
                                    "$sum": {
                                        "$cond": [{"$regexMatch": {"input": "$status", "regex": "Referred"}}, 1, 0]
                                    }
                                }
                            }}
                        ]
                    }
                }
            ]
            
            result = list(self.tickets.aggregate(pipeline))
            if not result:
                return {}
                
            data = result[0]
            
            claims = data.get("claims_stats", [{}])[0]
            
            return {
                "overdue_tickets": [
                    {**t, 
                     "has_unread_reply": bool(t.get("has_unread_reply", False)),
                     "has_unread_notification": bool(t.get("has_unread_notification", False))
                    } for t in data.get("overdue", [])
                ],
                "unread_tickets": [
                    {**t, 
                     "has_unread_reply": bool(t.get("has_unread_reply", False)),
                     "has_unread_notification": bool(t.get("has_unread_notification", False))
                    } for t in data.get("unread", [])
                ],
                "total_claims": claims.get("total", 0),
                "approved_claims": claims.get("approved", 0),
                "declined_claims": claims.get("declined", 0),
                "referred_claims": claims.get("referred", 0)
            }
            
        except Exception as e:
            logging.error(f"[DATABASE] Error getting dashboard stats: {e}")
            return {}
    
    def ticket_id_exists(self, ticket_id):
        """Fast check if ticket ID already exists (for duplicate checking)"""
        try:
            result = self.tickets.find_one({"ticket_id": ticket_id}, {"_id": 1})
            return result is not None
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to check ticket existence {ticket_id}: {e}")
            # Don't assume ID exists on database errors - raise exception to handle properly
            raise Exception(f"Database connectivity issue while checking ticket ID: {e}")
        except Exception as e:
            logging.error(f"Unexpected error checking ticket existence {ticket_id}: {e}")
            # Don't assume ID exists on database errors - raise exception to handle properly  
            raise Exception(f"Database error while checking ticket ID: {e}")

    def get_ticket_by_id(self, ticket_id):
        """Get ticket by ticket_id with assignment info including forwarded_to member"""
        try:
            pipeline = [
                {"$match": {"ticket_id": ticket_id}},
                {
                    "$lookup": {
                        "from": "ticket_assignments",
                        "localField": "ticket_id",
                        "foreignField": "ticket_id",
                        "as": "assignment"
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "assignment.member_id",
                        "foreignField": "_id",
                        "as": "assigned_member"
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "assignment.forwarded_from",
                        "foreignField": "_id",
                        "as": "forwarded_from_member"
                    }
                },
                # NEW: Lookup forwarded_to member from ticket document
                {
                    "$addFields": {
                        "forwarded_to_member_id": "$forwarded_to"
                    }
                },
                {
                    "$lookup": {
                        "from": "members",
                        "localField": "forwarded_to_member_id",
                        "foreignField": "_id",
                        "as": "forwarded_to_member"
                    }
                },
                {
                    "$project": {
                        "forwarded_to_member_id": 0
                    }
                }
            ]
            result = list(self.tickets.aggregate(pipeline))
            return result[0] if result else None
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get ticket {ticket_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting ticket {ticket_id}: {e}")
            return None
    
    def create_ticket(self, ticket_data):
        """Create a new ticket"""
        try:
            ticket_data['created_at'] = datetime.now()
            ticket_data['updated_at'] = datetime.now()
            ticket_data.setdefault('status', 'Open')
            ticket_data.setdefault('is_important', False)
            ticket_data.setdefault('has_unread_reply', False)
            
            result = self.tickets.insert_one(ticket_data)
            return result.inserted_id
        except pymongo.errors.DuplicateKeyError as e:
            # Check which field caused the duplicate key error
            error_msg = str(e)
            if "ticket_id" in error_msg:
                logging.error(f"Duplicate ticket ID {ticket_data.get('ticket_id')}: {e}")
                raise ValueError("Ticket ID already exists")
            elif "thread_id" in error_msg:
                logging.error(f"Duplicate thread ID {ticket_data.get('thread_id')}: {e}")
                raise ValueError("Thread ID already exists")
            else:
                logging.error(f"Duplicate key error: {e}")
                raise ValueError("Duplicate key constraint violated")
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to create ticket due to database operation failure: {e}")
            raise Exception(f"Database operation failed: {e}")
        except Exception as e:
            logging.error(f"Unexpected error creating ticket: {e}")
            raise Exception(f"Ticket creation failed: {e}")
    
    def update_ticket(self, ticket_id, update_data):
        """Update ticket by ticket_id"""
        try:
            update_data['updated_at'] = datetime.now()
            result = self.tickets.update_one(
                {"ticket_id": ticket_id},
                {"$set": update_data}
            )
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to update ticket {ticket_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error updating ticket {ticket_id}: {e}")
            raise
    
    def create_reply(self, reply_data):
        """Create a new reply"""
        try:
            reply_data['created_at'] = datetime.now()
            result = self.replies.insert_one(reply_data)
            return result.inserted_id
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to create reply: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error creating reply: {e}")
            raise
    
    def get_replies_by_ticket(self, ticket_id):
        """Get all replies for a ticket"""
        try:
            return list(self.replies.find(
                {"ticket_id": ticket_id}
            ).sort("created_at", 1))
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get replies for ticket {ticket_id}: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting replies: {e}")
            return []
    
    def get_member_by_user_id(self, user_id):
        """Get member by user_id"""
        try:
            return self.members.find_one({"user_id": user_id})
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get member by user_id {user_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting member: {e}")
            return None
    
    def get_member_by_id(self, member_id):
        """Get member by _id with in-memory caching"""
        # PERFORMANCE: Cache member lookups (called on every request via session)
        cache_key = f'member:{member_id}'
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(member_id):
                return None
            result = self.members.find_one({"_id": ObjectId(member_id)})
            if result:
                self._cache_set(cache_key, result, self._CACHE_TTL['member'])
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get member by id {member_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting member by id: {e}")
            return None
    
    def get_all_members(self):
        """Get all members with in-memory caching"""
        cached = self._cache_get('all_members')
        if cached is not None:
            return cached
        
        try:
            result = list(self.members.find().sort("name", 1))
            self._cache_set('all_members', result, self._CACHE_TTL['all_members'])
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get all members: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting members: {e}")
            return []
    
    def create_member(self, member_data):
        """Create a new member"""
        try:
            member_data['created_at'] = datetime.now()
            result = self.members.insert_one(member_data)
            self.invalidate_cache('all_members')  # Clear members cache
            return result.inserted_id
        except pymongo.errors.DuplicateKeyError as e:
            logging.error(f"Duplicate user_id: {e}")
            raise ValueError("User ID already exists")
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to create member: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error creating member: {e}")
            raise
    
    def assign_ticket(self, assignment_data):
        """Assign ticket to member with FIXED comprehensive error handling and persistence"""
        try:
            # Ensure required fields are set
            if 'assigned_at' not in assignment_data:
                assignment_data['assigned_at'] = datetime.now()
            
            # Validate required fields
            if not assignment_data.get('ticket_id'):
                raise ValueError("ticket_id is required")
            if not assignment_data.get('member_id'):
                raise ValueError("member_id is required")
            
            # Log the assignment attempt with detailed info
            ticket_id = assignment_data.get('ticket_id')
            member_id = assignment_data.get('member_id')
            logging.info(f"[TARGET] CREATING ASSIGNMENT: Ticket {ticket_id} -> Member {member_id} (type: {type(member_id).__name__})")
            logging.info(f"[INFO] Assignment Data: ticket_id={ticket_id}, member_id={member_id}, is_forwarded={assignment_data.get('is_forwarded', False)}")
            
            # Check for existing assignment and remove it first (ATOMIC OPERATION)
            existing = self.ticket_assignments.find_one({"ticket_id": ticket_id})
            if existing:
                logging.info(f"[CLEANUP] REMOVING EXISTING ASSIGNMENT for ticket {ticket_id}")
                self.ticket_assignments.delete_one({"ticket_id": ticket_id})
            
            # Ensure default visibility flags
            try:
                if assignment_data.get('is_forwarded'):
                    # Forwarded assignments start as unseen by assignee
                    assignment_data.setdefault('is_seen', False)
                    assignment_data.setdefault('seen_at', None)
                else:
                    # Takeover assignments are considered seen immediately
                    assignment_data.setdefault('is_seen', True)
                    assignment_data.setdefault('seen_at', datetime.now())
            except Exception:
                # If anything goes wrong, do not block the assignment flow
                pass

            # Insert the new assignment
            result = self.ticket_assignments.insert_one(assignment_data)
            
            # Verify insertion immediately
            if not result.inserted_id:
                raise Exception("Assignment insertion failed - no ID returned")
                
            # Double-check the assignment was saved correctly
            verification = self.ticket_assignments.find_one({"_id": result.inserted_id})
            if not verification:
                raise Exception("Assignment verification failed - record not found after insert")
            
            logging.info(f"[SUCCESS] ASSIGNMENT CREATED & VERIFIED: ID {result.inserted_id}")
            logging.info(f"[DEBUG] VERIFICATION DATA: {verification}")
            
            return result.inserted_id
                
        except pymongo.errors.DuplicateKeyError as e:
            logging.error(f"[ERROR] DUPLICATE ASSIGNMENT ERROR: {e}")
            # Try to handle duplicate by updating instead of creating
            try:
                logging.info(f"[RETRY] ATTEMPTING UPDATE INSTEAD OF INSERT")
                update_result = self.ticket_assignments.update_one(
                    {"ticket_id": assignment_data["ticket_id"]},
                    {"$set": assignment_data}
                )
                if update_result.modified_count > 0:
                    logging.info(f"[SUCCESS] UPDATED EXISTING ASSIGNMENT")
                    return "updated"
                else:
                    raise Exception("Update failed")
            except Exception as update_error:
                logging.error(f"[ERROR] UPDATE FAILED: {update_error}")
                raise ValueError("Assignment already exists and update failed")
        except pymongo.errors.OperationFailure as e:
            logging.error(f"[ERROR] DATABASE OPERATION FAILED: {e}")
            raise Exception(f"Database error: {str(e)}")
        except Exception as e:
            logging.error(f"[ERROR] UNEXPECTED ASSIGNMENT ERROR: {e}")
            raise Exception(f"Assignment failed: {str(e)}")

    def mark_assignment_seen(self, ticket_id, member_id):
        """Mark a forwarded assignment as seen by the assignee"""
        try:
            from bson.objectid import ObjectId
            update = {
                "$set": {
                    "is_seen": True,
                    "seen_at": datetime.now()
                }
            }
            query = {"ticket_id": ticket_id}
            # If member_id is a valid ObjectId, include it in the filter to be strict
            if ObjectId.is_valid(str(member_id)):
                query["member_id"] = ObjectId(str(member_id))
            self.ticket_assignments.update_one(query, update)
            return True
        except Exception as e:
            logging.error(f"Failed to mark assignment seen for ticket {ticket_id}: {e}")
            return False
    
    def get_assignment_by_ticket(self, ticket_id):
        """Get assignment info for a ticket"""
        try:
            return self.ticket_assignments.find_one({"ticket_id": ticket_id})
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get assignment for ticket {ticket_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting assignment: {e}")
            return None
    
    def remove_assignment(self, ticket_id, member_id):
        """Remove ticket assignment"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(member_id):
                return None
            result = self.ticket_assignments.delete_one({
                "ticket_id": ticket_id,
                "member_id": ObjectId(member_id)
            })
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to remove assignment: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error removing assignment: {e}")
            raise
    
    def add_ticket_metadata(self, ticket_id, key, value):
        """Add metadata for a ticket"""
        try:
            metadata = {
                "ticket_id": ticket_id,
                "key": key,
                "value": value,
                "created_at": datetime.now()
            }
            result = self.ticket_metadata.insert_one(metadata)
            return result.inserted_id
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to add metadata for ticket {ticket_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error adding metadata: {e}")
            raise
    
    def get_ticket_metadata(self, ticket_id):
        """Get all metadata for a ticket"""
        try:
            result = list(self.ticket_metadata.find({"ticket_id": ticket_id}))
            if result:
                logging.info(f"✅ Retrieved {len(result)} metadata entries from database for ticket {ticket_id}")
                # Also sync with in-memory storage
                for meta in result:
                    if meta.get('key') in ['technician_id', 'technician_name']:
                        self._set_in_memory_metadata(ticket_id, meta.get('key'), meta.get('value'))
                return result
            else:
                logging.info(f"📭 No metadata found in database for ticket {ticket_id}, checking in-memory storage")
                # Check in-memory storage as fallback
                return self._get_in_memory_metadata(ticket_id)
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get metadata for ticket {ticket_id}: {e}")
            # Fallback to in-memory storage
            return self._get_in_memory_metadata(ticket_id)
        except Exception as e:
            logging.error(f"Unexpected error getting metadata: {e}")
            # Fallback to in-memory storage
            return self._get_in_memory_metadata(ticket_id)
    
    def _get_in_memory_metadata(self, ticket_id):
        """Get metadata from in-memory storage when database fails"""
        global technician_assignments
        logging.info(f"🔍 Looking for ticket {ticket_id} in in-memory storage: {technician_assignments}")
        if ticket_id in technician_assignments:
            metadata = []
            for key, value in technician_assignments[ticket_id].items():
                metadata.append({
                    "ticket_id": ticket_id,
                    "key": key,
                    "value": value,
                    "updated_at": datetime.now()
                })
            logging.info(f"✅ Retrieved {len(metadata)} metadata entries from memory for ticket {ticket_id}")
            return metadata
        logging.info(f"❌ No metadata found in memory for ticket {ticket_id}")
        return []
    
    def set_ticket_metadata(self, ticket_id, key, value):
        """Set or update metadata for a ticket (upsert)"""
        try:
            # Use upsert to either update existing or insert new
            result = self.ticket_metadata.update_one(
                {"ticket_id": ticket_id, "key": key},
                {
                    "$set": {
                        "value": value,
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
            
            # Verify the operation was successful
            if result.upserted_id or result.modified_count > 0:
                logging.info(f"✅ Successfully saved metadata: {ticket_id}.{key} = {value}")
                # Also update in-memory storage as backup
                self._set_in_memory_metadata(ticket_id, key, value)
                return result.upserted_id or result.modified_count
            else:
                logging.warning(f"⚠️ Metadata operation returned no changes: {ticket_id}.{key} = {value}")
                # Fallback to in-memory storage
                return self._set_in_memory_metadata(ticket_id, key, value)
                
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to set metadata for ticket {ticket_id}: {e}")
            # Fallback to in-memory storage
            return self._set_in_memory_metadata(ticket_id, key, value)
        except Exception as e:
            logging.error(f"Unexpected error setting metadata: {e}")
            # Fallback to in-memory storage
            return self._set_in_memory_metadata(ticket_id, key, value)
    
    def _set_in_memory_metadata(self, ticket_id, key, value):
        """Fallback in-memory metadata storage when database fails"""
        global technician_assignments
        if ticket_id not in technician_assignments:
            technician_assignments[ticket_id] = {}
        technician_assignments[ticket_id][key] = value
        logging.info(f"💾 Stored metadata in memory: {ticket_id}.{key} = {value}")
        logging.info(f"🔍 Current in-memory storage: {technician_assignments}")
        return 1
    
    def delete_ticket_metadata(self, ticket_id, key):
        """Delete specific metadata key for a ticket"""
        try:
            result = self.ticket_metadata.delete_many({"ticket_id": ticket_id, "key": key})
            return result.deleted_count
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to delete metadata for ticket {ticket_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error deleting metadata: {e}")
            raise
    
    def add_common_document_metadata(self, document_id, key, value):
        """🚀 NEW: Add metadata to a common document (like ticket system)"""
        try:
            metadata = {
                'document_id': document_id,
                'key': key,
                'value': value,
                'created_at': datetime.now()
            }
            result = self.common_document_metadata.insert_one(metadata)
            logging.info(f"✅ Added metadata for document {document_id}: {key} = {value}")
            return str(result.inserted_id)
        except Exception as e:
            logging.error(f"❌ Error adding common document metadata: {e}")
            return None
    
    def search_tickets(self, query=None, status=None, priority=None, classification=None):
        """Search tickets with filters"""
        try:
            search_filter = {}
            
            if query:
                search_filter["$or"] = [
                    {"ticket_id": {"$regex": query, "$options": "i"}},
                    {"subject": {"$regex": query, "$options": "i"}},
                    {"body": {"$regex": query, "$options": "i"}},
                    {"name": {"$regex": query, "$options": "i"}},
                    {"email": {"$regex": query, "$options": "i"}}
                ]
            
            if status and status != 'All':
                search_filter["status"] = status
            
            if priority and priority != 'All':
                search_filter["priority"] = priority
                
            if classification and classification != 'All':
                search_filter["classification"] = classification
            
            return list(self.tickets.find(search_filter).sort("updated_at", -1).limit(1000))
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to search tickets: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error searching tickets: {e}")
            return []
    
    def get_all_tickets(self):
        """Get all tickets"""
        try:
            return list(self.tickets.find().sort("updated_at", -1))
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get all tickets: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting all tickets: {e}")
            return []
    
    def delete_ticket(self, ticket_id):
        """Delete a ticket and all related data"""
        try:
            # Start transaction-like operations
            ticket = self.get_ticket_by_id(ticket_id)
            if not ticket:
                logging.warning(f"Ticket {ticket_id} not found for deletion")
                return {'success': False, 'message': 'Ticket not found'}
            
            # Delete related data first
            # 1. Delete ticket assignments
            self.ticket_assignments.delete_many({'ticket_id': ticket_id})
            logging.info(f"Deleted assignments for ticket {ticket_id}")
            
            # 2. Delete ticket metadata
            self.ticket_metadata.delete_many({'ticket_id': ticket_id})
            logging.info(f"Deleted metadata for ticket {ticket_id}")
            
            # 3. Delete ticket replies
            self.replies.delete_many({'ticket_id': ticket_id})
            logging.info(f"Deleted replies for ticket {ticket_id}")
            
            # 4. Finally delete the ticket itself
            result = self.tickets.delete_one({'ticket_id': ticket_id})
            
            if result.deleted_count > 0:
                logging.info(f"Successfully deleted ticket {ticket_id}")
                return {'success': True, 'message': 'Ticket deleted successfully'}
            else:
                logging.error(f"Failed to delete ticket {ticket_id}")
                return {'success': False, 'message': 'Failed to delete ticket'}
                
        except Exception as e:
            logging.error(f"Error deleting ticket {ticket_id}: {e}")
            return {'success': False, 'message': f'Error deleting ticket: {str(e)}'}
    
    def soft_delete_ticket(self, ticket_id, deleted_by):
        """Soft delete a ticket (mark as deleted without removing from database)"""
        try:
            from datetime import datetime
            update_data = {
                'is_deleted': True,
                'deleted_at': datetime.now(),
                'deleted_by': deleted_by,
                'status': 'Deleted'
            }
            
            result = self.tickets.update_one(
                {'ticket_id': ticket_id},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logging.info(f"Successfully soft-deleted ticket {ticket_id}")
                return {'success': True, 'message': 'Ticket marked as deleted'}
            else:
                logging.error(f"Failed to soft-delete ticket {ticket_id}")
                return {'success': False, 'message': 'Ticket not found or already deleted'}
                
        except Exception as e:
            logging.error(f"Error soft-deleting ticket {ticket_id}: {e}")
            return {'success': False, 'message': f'Error deleting ticket: {str(e)}'}
            
    def restore_ticket(self, ticket_id):
        """Restore a soft-deleted ticket"""
        try:
            result = self.tickets.update_one(
                {'ticket_id': ticket_id},
                {
                    '$set': {'is_deleted': False, 'status': 'Open'},
                    '$unset': {'deleted_at': '', 'deleted_by': ''}
                }
            )
            
            if result.modified_count > 0:
                logging.info(f"Successfully restored ticket {ticket_id}")
                return {'success': True, 'message': 'Ticket restored successfully'}
            else:
                logging.error(f"Failed to restore ticket {ticket_id}")
                return {'success': False, 'message': 'Ticket not found'}
                
        except Exception as e:
            logging.error(f"Error restoring ticket {ticket_id}: {e}")
            return {'success': False, 'message': f'Error restoring ticket: {str(e)}'}
    
    def get_deleted_tickets(self):
        """Get all soft-deleted tickets"""
        try:
            return list(self.tickets.find({'is_deleted': True}).sort([("deleted_at", -1)]))
        except Exception as e:
            logging.error(f"Failed to get deleted tickets: {e}")
            return []

    def get_dashboard_stats(self):
        """Get statistics for dashboard"""
        try:
            total_tickets = self.tickets.count_documents({})
            
            # Status counts
            status_pipeline = [
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ]
            status_counts = {item["_id"]: item["count"] for item in self.tickets.aggregate(status_pipeline)}
            
            # Priority counts
            priority_pipeline = [
                {"$group": {"_id": "$priority", "count": {"$sum": 1}}}
            ]
            priority_counts = {item["_id"]: item["count"] for item in self.tickets.aggregate(priority_pipeline)}
            
            # Classification counts
            classification_pipeline = [
                {"$group": {"_id": "$classification", "count": {"$sum": 1}}}
            ]
            classification_counts = {item["_id"]: item["count"] for item in self.tickets.aggregate(classification_pipeline)}
            
            return {
                "total_tickets": total_tickets,
                "status_counts": status_counts,
                "priority_counts": priority_counts,
                "classification_counts": classification_counts
            }
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get dashboard stats: {e}")
            return {"total_tickets": 0, "status_counts": {}, "priority_counts": {}, "classification_counts": {}}
        except Exception as e:
            logging.error(f"Unexpected error getting dashboard stats: {e}")
            return {"total_tickets": 0, "status_counts": {}, "priority_counts": {}, "classification_counts": {}}

    # Status Management Methods
    def get_all_ticket_statuses(self):
        """Get all ticket statuses"""
        try:
            statuses = list(self.ticket_statuses.find({'is_active': True}).sort('order', 1))
            return statuses
        except Exception as e:
            logging.error(f"Error getting ticket statuses: {e}")
            return []
    
    def create_ticket_status(self, status_data):
        """Create a new ticket status"""
        try:
            status_data['created_at'] = datetime.now()
            status_data['is_active'] = True
            # Get the next order number
            max_order = self.ticket_statuses.find_one(sort=[("order", -1)])
            status_data['order'] = (max_order['order'] if max_order else 0) + 1
            
            result = self.ticket_statuses.insert_one(status_data)
            return result.inserted_id
        except Exception as e:
            logging.error(f"Error creating ticket status: {e}")
            raise
    
    def update_ticket_status_config(self, status_id, update_data):
        """Update ticket status configuration"""
        try:
            from bson.objectid import ObjectId
            update_data['updated_at'] = datetime.now()
            result = self.ticket_statuses.update_one(
                {'_id': ObjectId(status_id)},
                {'$set': update_data}
            )
            return result
        except Exception as e:
            logging.error(f"Error updating ticket status: {e}")
            raise
    
    def deactivate_ticket_status(self, status_id):
        """Deactivate a ticket status (soft delete)"""
        try:
            from bson.objectid import ObjectId
            result = self.ticket_statuses.update_one(
                {'_id': ObjectId(status_id)},
                {'$set': {'is_active': False, 'updated_at': datetime.now()}}
            )
            return result
        except Exception as e:
            logging.error(f"Error deactivating ticket status: {e}")
            raise
    
    def initialize_default_statuses(self):
        """Initialize default ticket statuses if none exist"""
        try:
            if self.ticket_statuses.count_documents({}) == 0:
                default_statuses = [
                    {'name': 'New', 'color': '#f59e0b', 'description': 'Newly created ticket', 'order': 1},
                    {'name': 'Form Sent', 'color': '#3b82f6', 'description': 'Initial form sent to customer', 'order': 2},
                    {'name': 'Awaiting Submission', 'color': '#8b5cf6', 'description': 'Waiting for customer submission', 'order': 3},
                    {'name': 'Under Review', 'color': '#f59e0b', 'description': 'Ticket under review', 'order': 4},
                    {'name': 'Info Requested', 'color': '#06b6d4', 'description': 'Additional information requested', 'order': 5},
                    {'name': 'Warranty Form Received', 'color': '#10b981', 'description': 'Warranty form has been received', 'order': 6},
                    {'name': 'Referred to Tech Director', 'color': '#8b5cf6', 'description': 'Escalated to technical director', 'order': 7},
                    {'name': 'Approved - Revisit Booked', 'color': '#10b981', 'description': 'Claim approved, revisit scheduled', 'order': 8},
                    {'name': 'Declined - Not Covered', 'color': '#ef4444', 'description': 'Claim declined, not under warranty', 'order': 9},
                    {'name': 'Closed', 'color': '#6b7280', 'description': 'Ticket resolved and closed', 'order': 10}
                ]
                
                for status in default_statuses:
                    status['created_at'] = datetime.now()
                    status['is_active'] = True
                    self.ticket_statuses.insert_one(status)
                
                logging.info(f"Initialized {len(default_statuses)} default ticket statuses")
        except Exception as e:
            logging.error(f"Error initializing default statuses: {e}")

    def get_tickets_by_status(self, status):
        """Get all tickets with a specific status"""
        try:
            tickets = list(self.tickets.find({"status": status}).sort("updated_at", -1))
            return tickets
        except Exception as e:
            logging.error(f"Error getting tickets by status {status}: {e}")
            raise
    
    # Technician Management Methods
    def get_all_technicians(self):
        """Get all active technicians with in-memory caching"""
        cached = self._cache_get('all_technicians')
        if cached is not None:
            return cached
        
        try:
            result = list(self.technicians.find({"is_active": True}).sort("name", 1))
            self._cache_set('all_technicians', result, self._CACHE_TTL['all_technicians'])
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get technicians: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting technicians: {e}")
            return []
    
    def create_technician(self, technician_data):
        """Create a new technician with name, role, and email"""
        try:
            # Validate required fields
            required_fields = ['name', 'role']
            for field in required_fields:
                if not technician_data.get(field):
                    raise ValueError(f"Missing required field: {field}")
            
            # Check for duplicate email if provided
            if technician_data.get('email'):
                existing = self.technicians.find_one({"email": technician_data['email']})
                if existing:
                    raise ValueError("A technician with this email already exists")
            
            # Ensure employee_id is set (can be empty string)
            if 'employee_id' not in technician_data:
                technician_data['employee_id'] = ''
            
            technician_data['created_at'] = datetime.now()
            technician_data['is_active'] = True
            result = self.technicians.insert_one(technician_data)
            self.invalidate_cache('all_technicians')
            return result.inserted_id
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to create technician: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error creating technician: {e}")
            raise
    
    def update_technician(self, technician_id, update_data):
        """Update technician by ID"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(technician_id):
                raise ValueError("Invalid technician ID")
            
            update_data['updated_at'] = datetime.now()
            result = self.technicians.update_one(
                {"_id": ObjectId(technician_id)},
                {"$set": update_data}
            )
            self.invalidate_cache('all_technicians')
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to update technician {technician_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error updating technician {technician_id}: {e}")
            raise
    
    def deactivate_technician(self, technician_id):
        """Deactivate a technician (soft delete)"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(technician_id):
                raise ValueError("Invalid technician ID")
            
            result = self.technicians.update_one(
                {"_id": ObjectId(technician_id)},
                {"$set": {"is_active": False, "updated_at": datetime.now()}}
            )
            self.invalidate_cache('all_technicians')
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to deactivate technician {technician_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error deactivating technician {technician_id}: {e}")
            raise
    
    def activate_technician(self, technician_id):
        """Activate a technician"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(technician_id):
                raise ValueError("Invalid technician ID")
            
            result = self.technicians.update_one(
                {"_id": ObjectId(technician_id)},
                {"$set": {"is_active": True, "updated_at": datetime.now()}}
            )
            return result
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to activate technician {technician_id}: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error activating technician {technician_id}: {e}")
            raise
    
    def get_technician_by_id(self, technician_id):
        """Get technician by ID"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(technician_id):
                return None
            return self.technicians.find_one({"_id": ObjectId(technician_id)})
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get technician {technician_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting technician {technician_id}: {e}")
            return None

    def get_technician_by_name(self, technician_name):
        """Get technician by name"""
        try:
            return self.technicians.find_one({"name": technician_name, "is_active": True})
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get technician by name {technician_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting technician by name {technician_name}: {e}")
            return None

    def get_technicians_summary(self):
        """Get summary of technicians for admin dashboard"""
        try:
            return list(self.technicians.find({}, {"name": 1, "role": 1, "is_active": 1}).sort("name", 1))
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get technicians summary: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting technicians summary: {e}")
            return []

    # ============ ROLES MANAGEMENT METHODS ============
    
    def get_all_roles(self):
        """Get all roles"""
        try:
            return list(self.roles.find().sort("name", 1))
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get all roles: {e}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error getting roles: {e}")
            return []
    
    def create_role(self, role_data):
        """Create a new role"""
        try:
            role_data['created_at'] = datetime.now()
            result = self.roles.insert_one(role_data)
            return result.inserted_id
        except pymongo.errors.DuplicateKeyError as e:
            logging.error(f"Duplicate role name: {e}")
            raise ValueError("Role name already exists")
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to create role: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error creating role: {e}")
            raise
    
    def get_role_by_id(self, role_id):
        """Get role by _id"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(role_id):
                return None
            return self.roles.find_one({"_id": ObjectId(role_id)})
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to get role by id {role_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting role: {e}")
            return None
    
    def update_role(self, role_id, update_data):
        """Update role by _id"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(role_id):
                return False
            
            update_data['updated_at'] = datetime.now()
            result = self.roles.update_one(
                {"_id": ObjectId(role_id)}, 
                {"$set": update_data}
            )
            return result.modified_count > 0
        except pymongo.errors.OperationFailure as e:
            logging.error(f"Failed to update role {role_id}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error updating role: {e}")
            return False
    
        def delete_role(self, role_id):
            """Delete role by _id"""
            try:
                from bson.objectid import ObjectId
                if not ObjectId.is_valid(role_id):
                    return False
                
                # Check if role is in use by any members
                members_with_role = self.members.count_documents({"role": self.get_role_by_id(role_id)["name"]})
                if members_with_role > 0:
                    raise ValueError("Cannot delete role: it is currently assigned to members")
                
                result = self.roles.delete_one({"_id": ObjectId(role_id)})
                return result.deleted_count > 0
            except pymongo.errors.OperationFailure as e:
                logging.error(f"Failed to delete role {role_id}: {e}")
                return False
            except Exception as e:
                logging.error(f"Unexpected error deleting role: {e}")
                return False
    
    # ============ COMMON DOCUMENTS MANAGEMENT METHODS ============
    
    def create_common_document(self, document_data, file_data=None):
        """Create a new common document with enhanced file storage"""
        try:
            # Set default timestamps if not provided
            if 'created_at' not in document_data:
                document_data['created_at'] = datetime.now()
            if 'updated_at' not in document_data:
                document_data['updated_at'] = datetime.now()
            
            document_data['created_by'] = document_data.get('created_by', 'System')
            document_data['download_count'] = 0
            
            # ENHANCED: Handle file data from enhanced document_data structure
            if document_data.get('has_file_data') and document_data.get('file_data'):
                # File data is already base64 encoded in document_data
                file_content_base64 = document_data['file_data']
                document_data['file_content'] = file_content_base64  # Keep for backward compatibility
                logging.info(f"✅ Using pre-encoded base64 data: {len(file_content_base64)} chars")
                
                # 🚀 ENHANCED DEBUGGING: Log what's being stored
                logging.info(f"📄 Document data before storage:")
                logging.info(f"  - has_file_data: {document_data.get('has_file_data')}")
                logging.info(f"  - file_data present: {'file_data' in document_data}")
                logging.info(f"  - file_content present: {'file_content' in document_data}")
                logging.info(f"  - file_data length: {len(document_data.get('file_data', ''))}")
                logging.info(f"  - file_content length: {len(document_data.get('file_content', ''))}")
                
            elif file_data:
                # Legacy support: convert binary content to base64
                import base64
                file_content_base64 = base64.b64encode(file_data['content']).decode('utf-8')
                document_data['file_content'] = file_content_base64
                document_data['file_data'] = file_content_base64  # Add to new structure
                document_data['file_size'] = len(file_data['content'])
                document_data['file_type'] = file_data.get('type', 'application/octet-stream')
                document_data['has_file_data'] = True
                logging.info(f"✅ Converted legacy file data to base64: {len(file_content_base64)} chars")
            else:
                logging.warning(f"⚠️ No file data provided for document: {document_data.get('name')}")
                logging.warning(f"⚠️ has_file_data: {document_data.get('has_file_data')}")
                logging.warning(f"⚠️ file_data present: {'file_data' in document_data}")
                logging.warning(f"⚠️ file_content present: {'file_content' in document_data}")
            
            result = self.common_documents.insert_one(document_data)
            logging.info(f"✅ Created common document: {document_data.get('name')} with ID: {result.inserted_id}")
            
            # 🚀 ENHANCED DEBUGGING: Verify what was actually stored
            stored_doc = self.common_documents.find_one({'_id': result.inserted_id})
            if stored_doc:
                logging.info(f"📄 Verification of stored document:")
                logging.info(f"  - Stored has_file_data: {stored_doc.get('has_file_data')}")
                logging.info(f"  - Stored file_data present: {'file_data' in stored_doc}")
                logging.info(f"  - Stored file_content present: {'file_content' in stored_doc}")
                if 'file_data' in stored_doc:
                    logging.info(f"  - Stored file_data length: {len(stored_doc.get('file_data', ''))}")
                if 'file_content' in stored_doc:
                    logging.info(f"  - Stored file_content length: {len(stored_doc.get('file_content', ''))}")
            
            return str(result.inserted_id)
        except Exception as e:
            logging.error(f"❌ Error creating common document: {e}")
            raise
    
    def get_all_common_documents(self):
        """Get all common documents"""
        try:
            result = list(self.common_documents.find({}, {
                '_id': 1,
                'name': 1,
                'type': 1,
                'description': 1,
                'file_name': 1,
                'file_url': 1,
                'created_at': 1,
                'updated_at': 1,
                'created_by': 1,
                'download_count': 1,
                'file_size': 1,
                'file_type': 1,
                'has_file_data': 1,
                'file_content': 1,  # Include the actual file content
                'file_data': 1      # Include the enhanced file data structure
            }).sort('created_at', -1))
            
            # Convert ObjectId to string for JSON serialization
            for doc in result:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                if 'created_at' in doc:
                    doc['created_at'] = doc['created_at'].isoformat()
                if 'updated_at' in doc:
                    doc['updated_at'] = doc['updated_at'].isoformat()
            
            return result
        except Exception as e:
            logging.error(f"❌ Error getting common documents: {e}")
            return []
    
    def get_common_document_by_id(self, document_id):
        """Get a specific common document by ID"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return None
            result = self.common_documents.find_one({'_id': ObjectId(document_id)})
            
            if result:
                # Convert ObjectId to string for JSON serialization
                result['_id'] = str(result['_id'])
                if 'created_at' in result:
                    result['created_at'] = result['created_at'].isoformat()
                if 'updated_at' in result:
                    result['updated_at'] = result['updated_at'].isoformat()
                
                # Log file data availability for debugging
                has_file_content = 'file_content' in result and result['file_content']
                has_file_data = 'file_data' in result and result['file_data']
                logging.info(f"📄 Document {document_id} file data: content={has_file_content}, data={has_file_data}")
            
            return result
        except Exception as e:
            logging.error(f"❌ Error getting common document {document_id}: {e}")
            return None
    
    
    def update_common_document(self, document_id, update_data):
        """Update a common document"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False
                
            update_data['updated_at'] = datetime.now()
            
            result = self.common_documents.update_one(
                {'_id': ObjectId(document_id)},
                {'$set': update_data}
            )
            
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"❌ Error updating common document {document_id}: {e}")
            return False

    # ============ SYSTEM SETTINGS METHODS ============

    def get_system_settings(self):
        """Get system-wide settings with in-memory caching (avoids DB hit on every request)"""
        # PERFORMANCE: Check cache first
        cached = self._cache_get('system_settings')
        if cached is not None:
            return cached
        
        try:
            if not hasattr(self, 'settings'):
                self.settings = self.db.settings
            
            settings = self.settings.find_one({'_id': 'global_settings'})
            
            if not settings:
                default_settings = {
                    '_id': 'global_settings',
                    'show_background': True,
                    'updated_at': datetime.now(),
                    'updated_by': 'system'
                }
                self.settings.insert_one(default_settings)
                settings = default_settings
            
            self._cache_set('system_settings', settings, self._CACHE_TTL['system_settings'])
            return settings
        except Exception as e:
            logging.error(f"Error getting system settings: {e}")
            fallback = {'show_background': True}
            # Cache the fallback too so we don't spam the DB on every request during outage
            self._cache_set('system_settings', fallback, 30)  # shorter TTL for fallback
            return fallback

    def update_system_settings(self, updates):
        """Update system settings"""
        try:
            if not updates:
                return False
                
            if not hasattr(self, 'settings'):
                self.settings = self.db.settings
                
            updates['updated_at'] = datetime.now()
            
            result = self.settings.update_one(
                {'_id': 'global_settings'},
                {'$set': updates},
                upsert=True
            )
            
            self.invalidate_cache('system_settings')  # Clear cache so next request gets fresh data
            return True
        except Exception as e:
            logging.error(f"❌ Error updating system settings: {e}")
            return False
        """Update a common document"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False
                
            update_data['updated_at'] = datetime.now()
            
            result = self.common_documents.update_one(
                {'_id': ObjectId(document_id)},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logging.info(f"✅ Updated common document: {document_id}")
                return True
            else:
                logging.warning(f"⚠️ No changes made to common document: {document_id}")
                return False
        except Exception as e:
            logging.error(f"❌ Error updating common document {document_id}: {e}")
            raise
    
    def delete_common_document(self, document_id):
        """Delete a common document"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False
                
            result = self.common_documents.delete_one({'_id': ObjectId(document_id)})
            
            if result.deleted_count > 0:
                logging.info(f"✅ Deleted common document: {document_id}")
                return True
            else:
                logging.warning(f"⚠️ No common document found to delete: {document_id}")
                return False
        except Exception as e:
            logging.error(f"❌ Error deleting common document {document_id}: {e}")
            raise
    
    def update_common_document(self, document_id, update_data):
        """🚀 NEW: Update a common document with new data"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False, "Invalid document ID format"
            
            # Add updated timestamp
            update_data['updated_at'] = datetime.now()
            
            result = self.common_documents.update_one(
                {'_id': ObjectId(document_id)},
                {'$set': update_data}
            )
            
            if result.modified_count > 0:
                logging.info(f"✅ Successfully updated common document {document_id}")
                return True, f"Updated {result.modified_count} document(s)"
            else:
                logging.warning(f"⚠️ No changes made to common document {document_id}")
                return True, "No changes made"
                
        except Exception as e:
            logging.error(f"❌ Error updating common document {document_id}: {e}")
            return False, f"Update error: {str(e)}"
    
    def increment_document_download_count(self, document_id):
        """Increment download count for a document"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False
                
            result = self.common_documents.update_one(
                {'_id': ObjectId(document_id)},
                {'$inc': {'download_count': 1}}
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"❌ Error incrementing download count for document {document_id}: {e}")
            return False
    
    def get_document_file_content(self, document_id):
        """Get the file content of a document for download with comprehensive validation"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                logging.warning(f"Invalid ObjectId format: {document_id}")
                return None
                
            logging.info(f"Looking for document {document_id} file content")
            
            result = self.common_documents.find_one(
                {'_id': ObjectId(document_id)},
                {'file_content': 1, 'file_data': 1, 'file_name': 1, 'file_type': 1, 'name': 1, 'file_size': 1}
            )
            
            logging.info(f"Database query result: {result}")
            
            if result:
                logging.info(f"Document found, checking for file content fields")
                logging.info(f"Available fields: {list(result.keys())}")
                
                # 🚀 ENHANCED: Check both file_content (legacy) and file_data (new) fields
                file_content_base64 = None
                
                # First try the new file_data field
                if 'file_data' in result and result['file_data']:
                    file_content_base64 = result['file_data']
                    logging.info(f"✅ Using new file_data field: {len(file_content_base64)} chars")
                # Fallback to legacy file_content field
                elif 'file_content' in result and result['file_content']:
                    file_content_base64 = result['file_content']
                    logging.info(f"✅ Using legacy file_content field: {len(file_content_base64)} chars")
                
                if file_content_base64:
                    # 🚨 CRITICAL VALIDATION: Ensure file_content is not empty
                    if not file_content_base64:
                        logging.error(f"❌ CRITICAL: file_content is empty for document {document_id}")
                        return None
                    
                    # 🚨 CRITICAL VALIDATION: Ensure file_content is a string
                    if not isinstance(file_content_base64, str):
                        logging.error(f"❌ CRITICAL: file_content is not a string for document {document_id}, type: {type(file_content_base64)}")
                        return None
                    
                    # 🚀 ENHANCED: Validate base64 format
                    try:
                        # Test base64 decode to ensure it's valid
                        test_decode = base64.b64decode(file_content_base64)
                        file_size = len(test_decode)
                        logging.info(f"✅ Base64 validation passed for document {document_id}: {file_size} bytes")
                    except Exception as base64_error:
                        logging.error(f"❌ CRITICAL: Invalid base64 content for document {document_id}: {base64_error}")
                        return None
                    
                    # 🚀 ENHANCED: Get file information with validation
                    file_name = result.get('file_name', 'document')
                    file_type = result.get('file_type', 'application/octet-stream')
                    
                    # 🚨 CRITICAL VALIDATION: Ensure file_type is valid
                    if not file_type or file_type == 'application/octet-stream':
                        # Try to detect from filename
                        import mimetypes
                        detected_type, _ = mimetypes.guess_type(file_name)
                        if detected_type:
                            file_type = detected_type
                            logging.info(f"📄 MIME type corrected for {file_name}: {file_type}")
                    
                    return {
                        'content': file_content_base64,  # 🚀 VALIDATED BASE64 STRING
                        'file_name': file_name,
                        'file_type': file_type,
                        'name': result.get('name', 'Document'),
                        'file_size': file_size,  # 🚀 ACTUAL DECODED SIZE
                        'is_base64': True,  # 🚀 VALIDATION FLAG
                        'is_validated': True  # 🚀 NEW: Indicates content was validated
                    }
                else:
                    logging.warning(f"Document exists but no file content found in either file_data or file_content fields")
                    logging.warning(f"Document fields: {list(result.keys())}")
                    logging.warning(f"file_data present: {'file_data' in result}")
                    logging.warning(f"file_content present: {'file_content' in result}")
                    if 'file_data' in result:
                        logging.warning(f"file_data value: {type(result['file_data'])} - {len(str(result['file_data'])) if result['file_data'] else 'None'}")
                    if 'file_content' in result:
                        logging.warning(f"file_content value: {type(result['file_content'])} - {len(str(result['file_content'])) if result['file_content'] else 'None'}")
                    return None
            else:
                logging.warning(f"No document found with ID: {document_id}")
                return None
            
        except Exception as e:
            logging.error(f"❌ Error getting document file content {document_id}: {e}")
            return None
    
    def validate_document_integrity(self, document_id):
        """🚀 ENHANCED: Validate document file integrity and fix issues if possible"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False, "Invalid document ID format"
            
            # Get document data
            document = self.common_documents.find_one({'_id': ObjectId(document_id)})
            if not document:
                return False, "Document not found"
            
            # 🚀 ENHANCED: Check both file_content and file_data fields
            has_file_content = 'file_content' in document and document['file_content']
            has_file_data = 'file_data' in document and document['file_data']
            
            logging.info(f"📄 Document {document_id} integrity check:")
            logging.info(f"  - Has file_content: {has_file_content}")
            logging.info(f"  - Has file_data: {has_file_data}")
            
            if not has_file_content and not has_file_data:
                return False, "Document has no file content in either field"
            
            # Use whichever field has content
            file_content = document.get('file_data') or document.get('file_content')
            
            # Validate base64 format
            try:
                decoded_content = base64.b64decode(file_content)
                if not decoded_content:
                    return False, "File content is empty after decoding"
                
                # Check file size
                file_size = len(decoded_content)
                if file_size == 0:
                    return False, "File size is 0 bytes"
                if file_size > 100 * 1024 * 1024:  # 100MB limit
                    return False, f"File too large: {file_size} bytes"
                
                # Validate MIME type
                file_name = document.get('file_name', 'document')
                file_type = document.get('file_type', 'application/octet-stream')
                
                if not file_type or file_type == 'application/octet-stream':
                    import mimetypes
                    detected_type, _ = mimetypes.guess_type(file_name)
                    if detected_type:
                        # Update the document with correct MIME type
                        self.common_documents.update_one(
                            {'_id': ObjectId(document_id)},
                            {'$set': {'file_type': detected_type}}
                        )
                        logging.info(f"📄 Updated MIME type for {file_name}: {detected_type}")
                
                return True, f"Document integrity validated: {file_size} bytes, type: {file_type}"
                
            except Exception as e:
                return False, f"Base64 validation failed: {str(e)}"
                
        except Exception as e:
            logging.error(f"❌ Error validating document integrity {document_id}: {e}")
            return False, f"Validation error: {str(e)}"
    
    def repair_document_file_content(self, document_id):
        """🚀 NEW: Attempt to repair document file content by checking disk files"""
        try:
            from bson.objectid import ObjectId
            if not ObjectId.is_valid(document_id):
                return False, "Invalid document ID format"
            
            # Get document data
            document = self.common_documents.find_one({'_id': ObjectId(document_id)})
            if not document:
                return False, "Document not found"
            
            logging.info(f"🔧 Attempting to repair document {document_id}: {document.get('name', 'Unknown')}")
            
            # Check if document already has valid file content
            is_valid, message = self.validate_document_integrity(document_id)
            if is_valid:
                return True, f"Document is already valid: {message}"
            
            # Try to find the file on disk
            file_path = document.get('file_path')
            if file_path and os.path.exists(file_path):
                logging.info(f"🔧 Found file on disk: {file_path}")
                
                try:
                    # Read file from disk
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    
                    if file_content:
                        # Convert to base64
                        import base64
                        file_data_base64 = base64.b64encode(file_content).decode('utf-8')
                        
                        # Update document with file content
                        update_data = {
                            'file_content': file_data_base64,
                            'file_data': file_data_base64,
                            'has_file_data': True,
                            'file_size': len(file_content)
                        }
                        
                        # Update MIME type if not set
                        if not document.get('file_type') or document.get('file_type') == 'application/octet-stream':
                            import mimetypes
                            detected_type, _ = mimetypes.guess_type(document.get('file_name', ''))
                            if detected_type:
                                update_data['file_type'] = detected_type
                        
                        result = self.common_documents.update_one(
                            {'_id': ObjectId(document_id)},
                            {'$set': update_data}
                        )
                        
                        if result.modified_count > 0:
                            logging.info(f"✅ Successfully repaired document {document_id}")
                            return True, f"Document repaired: {len(file_content)} bytes restored from disk"
                        else:
                            return False, "Failed to update document in database"
                    else:
                        return False, "File on disk is empty"
                        
                except Exception as e:
                    logging.error(f"❌ Error reading file from disk: {e}")
                    return False, f"Error reading file from disk: {str(e)}"
            else:
                logging.warning(f"⚠️ No file path found or file doesn't exist on disk")
                return False, "No file path found or file doesn't exist on disk"
                
        except Exception as e:
            logging.error(f"❌ Error repairing document {document_id}: {e}")
            return False, f"Repair error: {str(e)}"
    
    def initialize_default_roles(self):
        """Initialize default roles if they don't exist - Simplified to 3 core roles"""
        try:
            default_roles = [
                {
                    "name": "Administrator",
                    "description": "Full system access with user management and configuration controls",
                    "permissions": ["full_access", "user_management", "system_config", "ticket_management"],
                    "level": 1,
                    "color": "#6366f1"
                },
                {
                    "name": "Technical Director", 
                    "description": "Technical oversight with referred ticket review and assessment",
                    "permissions": ["referred_tickets", "technical_assessment", "reports", "ticket_management"],
                    "level": 2,
                    "color": "#f59e0b"
                },
                {
                    "name": "User",
                    "description": "IT support team members with ticket management and technical assistance",
                    "permissions": ["ticket_management", "it_support", "portal_assistance", "technical_help"],
                    "level": 3,
                    "color": "#10b981"
                }
            ]
            
            for role_data in default_roles:
                existing_role = self.roles.find_one({"name": role_data["name"]})
                if not existing_role:
                    role_data['created_at'] = datetime.now()
                    role_data['is_default'] = True
                    self.roles.insert_one(role_data)
                    logging.info(f"Created default role: {role_data['name']}")
                    
        except Exception as e:
            logging.error(f"Error initializing default roles: {e}")
    
    # ============ ENHANCED WARRANTY AND ATTACHMENT ANALYTICS ============
    
    def get_warranty_analytics(self):
        """Get comprehensive warranty detection analytics"""
        try:
            # Basic warranty statistics
            total_tickets = self.tickets.count_documents({})
            warranty_tickets = self.tickets.count_documents({"has_warranty": True})
            attachment_tickets = self.tickets.count_documents({"has_attachments": True})
            
            # Warranty forms distribution
            warranty_forms_pipeline = [
                {"$match": {"has_warranty": True}},
                {"$group": {
                    "_id": "$warranty_forms_count",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
            warranty_forms_dist = list(self.tickets.aggregate(warranty_forms_pipeline))
            
            # Processing method statistics
            processing_methods_pipeline = [
                {"$group": {
                    "_id": "$processing_method",
                    "count": {"$sum": 1},
                    "warranty_count": {
                        "$sum": {"$cond": [{"$eq": ["$has_warranty", True]}, 1, 0]}
                    }
                }}
            ]
            processing_methods = list(self.tickets.aggregate(processing_methods_pipeline))
            
            # Monthly warranty trend
            monthly_warranty_pipeline = [
                {"$match": {"has_warranty": True}},
                {"$group": {
                    "_id": {
                        "year": {"$year": "$created_at"},
                        "month": {"$month": "$created_at"}
                    },
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id.year": -1, "_id.month": -1}},
                {"$limit": 12}
            ]
            monthly_warranty = list(self.tickets.aggregate(monthly_warranty_pipeline))
            
            # Warranty detection by status
            warranty_by_status_pipeline = [
                {"$match": {"has_warranty": True}},
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]
            warranty_by_status = list(self.tickets.aggregate(warranty_by_status_pipeline))
            
            return {
                "total_tickets": total_tickets,
                "warranty_tickets": warranty_tickets,
                "attachment_tickets": attachment_tickets,
                "warranty_percentage": (warranty_tickets / total_tickets * 100) if total_tickets > 0 else 0,
                "attachment_percentage": (attachment_tickets / total_tickets * 100) if total_tickets > 0 else 0,
                "warranty_forms_distribution": warranty_forms_dist,
                "processing_methods": processing_methods,
                "monthly_warranty_trend": monthly_warranty,
                "warranty_by_status": warranty_by_status
            }
            
        except Exception as e:
            logging.error(f"Error getting warranty analytics: {e}")
            return {
                "total_tickets": 0,
                "warranty_tickets": 0,
                "attachment_tickets": 0,
                "warranty_percentage": 0,
                "attachment_percentage": 0,
                "warranty_forms_distribution": [],
                "processing_methods": [],
                "monthly_warranty_trend": [],
                "warranty_by_status": []
            }
    
    def get_attachment_analytics(self):
        """Get comprehensive attachment analytics"""
        try:
            # Attachment size statistics
            attachment_size_pipeline = [
                {"$match": {"has_attachments": True}},
                {"$group": {
                    "_id": None,
                    "total_size": {"$sum": "$attachment_total_size"},
                    "avg_size": {"$avg": "$attachment_total_size"},
                    "max_size": {"$max": "$attachment_total_size"},
                    "total_tickets": {"$sum": 1}
                }}
            ]
            size_stats = list(self.tickets.aggregate(attachment_size_pipeline))
            
            # Attachment count distribution
            attachment_count_pipeline = [
                {"$match": {"has_attachments": True}},
                {"$group": {
                    "_id": "$total_attachments",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
            attachment_count_dist = list(self.tickets.aggregate(attachment_count_pipeline))
            
            return {
                "size_statistics": size_stats[0] if size_stats else {},
                "attachment_count_distribution": attachment_count_dist
            }
            
        except Exception as e:
            logging.error(f"Error getting attachment analytics: {e}")
            return {
                "size_statistics": {},
                "attachment_count_distribution": []
            }
    
    def update_ticket_warranty_metadata(self, ticket_id, warranty_data):
        """Update ticket with enhanced warranty metadata"""
        try:
            result = self.tickets.update_one(
                {"ticket_id": ticket_id},
                {"$set": {
                    "has_warranty": warranty_data.get("has_warranty", False),
                    "has_attachments": warranty_data.get("has_attachments", False),
                    "warranty_forms_count": warranty_data.get("warranty_forms_count", 0),
                    "total_attachments": warranty_data.get("total_attachments", 0),
                    "attachment_total_size": warranty_data.get("attachment_total_size", 0),
                    "processing_method": warranty_data.get("processing_method", "manual"),
                    "warranty_updated_at": datetime.now()
                }}
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Error updating warranty metadata for {ticket_id}: {e}")
            return False

    def update_replies_add_sender_field(self):
        """Migration: Add 'sender' field to replies that don't have it"""
        try:
            # Update replies without 'sender' field - assume they are support replies
            result = self.replies.update_many(
                {"sender": {"$exists": False}},  # Find replies without sender field
                {"$set": {"sender": "support"}}  # Set as support replies
            )
            if result.modified_count > 0:
                logging.info(f"Updated {result.modified_count} replies with missing 'sender' field")
            return result.modified_count
        except Exception as e:
            logging.error(f"Error updating replies sender field: {e}")
            return 0

# Global database instance
db = None

# Temporary in-memory storage for technician assignments (for testing without database)
technician_assignments = {}

def get_db():
    """Get database instance with connection validation"""
    global db
    try:
        if db is None:
            db = MongoDB()
        return db
    except Exception as e:
        logging.error(f"Failed to get database connection: {e}")
        raise 