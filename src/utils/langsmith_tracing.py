"""
LangSmith integration for user interaction tracing in the Business Document Search system.

This module provides focused tracing for user queries and AI responses in the Streamlit interface,
giving insights into user behavior and system performance without tracing internal processing details.
"""

import os
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict

try:
    from langsmith import Client
    from langsmith.run_helpers import traceable
    LANGSMITH_AVAILABLE = True
except ImportError:
    LANGSMITH_AVAILABLE = False
    
    # Fallback decorator when LangSmith not available
    def traceable(name: str = None, **kwargs):
        def decorator(func):
            return func
        return decorator

@dataclass
class UserInteractionMetrics:
    """Metrics captured for user interactions"""
    query: str
    response_length: int
    search_time_seconds: float
    documents_found: int
    filters_applied: Dict[str, Any]
    timestamp: str
    session_id: Optional[str] = None
    
class LangSmithUserTracker:
    """
    Focused LangSmith integration for tracking user interactions in the document search system.
    
    Only traces:
    - User search queries
    - AI-generated responses 
    - Key performance metrics
    - User session information
    """
    
    def __init__(self, project_name: str = "business-document-search", enabled: bool = True):
        self.enabled = enabled and LANGSMITH_AVAILABLE
        self.project_name = project_name
        self.client = None
        
        if self.enabled:
            try:
                # Initialize LangSmith client
                api_key = os.getenv("LANGSMITH_API_KEY")
                if api_key:
                    self.client = Client(api_key=api_key)
                    print(f"✅ LangSmith user interaction tracing enabled for project: {project_name}")
                else:
                    print("⚠️  LANGSMITH_API_KEY not found - user tracing disabled")
                    self.enabled = False
            except Exception as e:
                print(f"⚠️  LangSmith initialization failed: {e} - user tracing disabled")
                self.enabled = False
        else:
            if not LANGSMITH_AVAILABLE:
                print("📦 LangSmith not installed - user tracing disabled")
            else:
                print("🔕 LangSmith user tracing disabled by configuration")
    
    @traceable(name="user_document_search")
    def trace_user_search(
        self,
        query: str,
        response: str,
        search_time: float,
        documents_found: int,
        filters: Optional[Dict] = None,
        session_id: Optional[str] = None,
        additional_metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Trace a complete user search interaction from query to response.
        
        Args:
            query: User's search query
            response: AI-generated response shown to user
            search_time: Time taken to generate response (seconds)
            documents_found: Number of source documents found
            filters: Applied search filters
            session_id: User session identifier
            additional_metadata: Extra metadata to include
            
        Returns:
            Interaction metrics dictionary
        """
        interaction_data = {
            "query": query,
            "response": response,
            "response_length": len(response),
            "search_time_seconds": round(search_time, 2),
            "documents_found": documents_found,
            "filters_applied": filters or {},
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            **(additional_metadata or {})
        }
        
        if self.enabled:
            try:
                # Create run with user interaction context
                run_data = {
                    "name": "user_document_search",
                    "run_type": "chain",  # Required parameter for LangSmith API
                    "inputs": {
                        "user_query": query,
                        "applied_filters": filters or {},
                        "session_id": session_id
                    },
                    "outputs": {
                        "ai_response": response,
                        "documents_found": documents_found
                    },
                    "extra": {
                        "search_time_seconds": search_time,
                        "response_length": len(response),
                        "timestamp": interaction_data["timestamp"],
                        "interaction_type": "document_search",
                        **(additional_metadata or {})
                    }
                }
                
                # Log to LangSmith
                self.client.create_run(**run_data)
                
            except Exception as e:
                print(f"⚠️  Failed to log user interaction to LangSmith: {e}")
        
        return interaction_data
    
    @traceable(name="user_query_analysis")
    def trace_query_patterns(
        self,
        query: str,
        query_type: str,
        extracted_entities: Dict[str, List[str]],
        filters_suggested: Dict[str, Any],
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trace user query patterns and analysis for understanding user behavior.
        
        Args:
            query: Original user query
            query_type: Classified query type (e.g., "pricing", "vendor", "contract")
            extracted_entities: Entities found in query (vendors, clients, etc.)
            filters_suggested: Filters that could be applied based on query
            session_id: User session identifier
        """
        analysis_data = {
            "query": query,
            "query_type": query_type,
            "extracted_entities": extracted_entities,
            "filters_suggested": filters_suggested,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id
        }
        
        if self.enabled:
            try:
                run_data = {
                    "name": "user_query_analysis",
                    "run_type": "chain",  # Required parameter for LangSmith API
                    "inputs": {"user_query": query},
                    "outputs": {
                        "query_type": query_type,
                        "entities": extracted_entities,
                        "suggested_filters": filters_suggested
                    },
                    "extra": {
                        "timestamp": analysis_data["timestamp"],
                        "session_id": session_id,
                        "analysis_type": "query_understanding"
                    }
                }
                
                self.client.create_run(**run_data)
                
            except Exception as e:
                print(f"⚠️  Failed to log query analysis to LangSmith: {e}")
        
        return analysis_data
    
    def trace_user_session(
        self,
        session_id: str,
        session_duration: float,
        total_queries: int,
        unique_filters_used: List[str],
        most_common_query_types: List[str]
    ) -> Dict[str, Any]:
        """
        Trace user session summary for understanding usage patterns.
        
        Args:
            session_id: User session identifier
            session_duration: Total session time in seconds
            total_queries: Number of queries in session
            unique_filters_used: List of filter types used
            most_common_query_types: Most frequent query types in session
        """
        session_data = {
            "session_id": session_id,
            "session_duration_seconds": round(session_duration, 2),
            "total_queries": total_queries,
            "unique_filters_used": unique_filters_used,
            "most_common_query_types": most_common_query_types,
            "timestamp": datetime.now().isoformat()
        }
        
        if self.enabled:
            try:
                run_data = {
                    "name": "user_session_summary",
                    "run_type": "chain",  # Required parameter for LangSmith API
                    "inputs": {"session_id": session_id},
                    "outputs": session_data,
                    "extra": {
                        "session_type": "user_interaction_summary",
                        "timestamp": session_data["timestamp"]
                    }
                }
                
                self.client.create_run(**run_data)
                
            except Exception as e:
                print(f"⚠️  Failed to log session summary to LangSmith: {e}")
        
        return session_data


# Global tracker instance
_user_tracker = None

def get_user_tracker() -> LangSmithUserTracker:
    """Get or create the global user tracker instance"""
    global _user_tracker
    if _user_tracker is None:
        # Load settings
        enabled = os.getenv("LANGSMITH_ENABLED", "false").lower() == "true"
        project = os.getenv("LANGSMITH_PROJECT", "business-document-search")
        _user_tracker = LangSmithUserTracker(project_name=project, enabled=enabled)
    return _user_tracker

def trace_user_search(*args, **kwargs):
    """Convenience function for tracing user searches"""
    return get_user_tracker().trace_user_search(*args, **kwargs)

def trace_query_patterns(*args, **kwargs):
    """Convenience function for tracing query patterns"""
    return get_user_tracker().trace_query_patterns(*args, **kwargs)

def trace_user_session(*args, **kwargs):
    """Convenience function for tracing user sessions"""
    return get_user_tracker().trace_user_session(*args, **kwargs)


@traceable(name="retrieval_results")
def trace_retrieval_results(
    query: str,
    chunks: List[Dict[str, Any]],
    namespace: str,
    provider: str,
    model: str,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Trace Pinecone retrieval results, including chunk metadata and previews.

    Args:
        query: Original user query.
        chunks: List of chunk dictionaries (metadata + text preview).
        namespace: Pinecone namespace searched.
        provider: Active LLM provider (e.g., openai, anthropic).
        model: Active LLM model.
        session_id: Optional user session identifier.
    """
    tracker = get_user_tracker()
    payload = {
        "query": query,
        "chunks": chunks,
        "namespace": namespace,
        "provider": provider,
        "model": model,
        "session_id": session_id,
        "timestamp": datetime.now().isoformat(),
    }

    if tracker.enabled:
        try:
            tracker.client.create_run(
                name="retrieval_results",
                run_type="tool",
                inputs={
                    "query": query,
                    "namespace": namespace,
                },
                outputs={"chunks": chunks},
                extra={
                    "provider": provider,
                    "model": model,
                    "session_id": session_id,
                    "timestamp": payload["timestamp"],
                },
            )
        except Exception as exc:
            print(f"⚠️  Failed to log retrieval to LangSmith: {exc}")

    return payload