"""
AetherDB Chat Message History for LangChain.
Persists conversational message history directly to AetherDB Agent State.
"""

from typing import List, Optional, Sequence, Union
import json

from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.messages import (
    BaseMessage,
    messages_from_dict,
    messages_to_dict,
)

# Import AetherDB from core Python SDK
try:
    from aetherdb import AetherDB
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python")))
    from aetherdb import AetherDB


class AetherDBChatMessageHistory(BaseChatMessageHistory):
    """
    Chat message history stored in AetherDB Agent State namespace.

    Each agent ID has its own persistent, isolated conversation history state.
    """

    def __init__(
        self,
        agent_id: str,
        db: Optional[Union[AetherDB, str]] = None,
        session_key: str = "chat_history",
        endpoint: str = "http://127.0.0.1:8301",
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ):
        if not agent_id or not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id must be a non-empty string")

        self.agent_id = agent_id.strip()
        self.session_key = session_key.strip()

        if isinstance(db, AetherDB):
            self.db = db
        elif isinstance(db, str):
            self.db = AetherDB(endpoint=db, api_key=api_key, tenant_id=tenant_id)
        else:
            self.db = AetherDB(endpoint=endpoint, api_key=api_key, tenant_id=tenant_id)

        self._agent = self.db.agent(self.agent_id)

    @property
    def messages(self) -> List[BaseMessage]:
        """Retrieve stored chat messages for this agent."""
        raw_state = self._agent.state.get(self.session_key)
        if not raw_state:
            return []

        if isinstance(raw_state, str):
            try:
                raw_state = json.loads(raw_state)
            except Exception:
                return []

        if not isinstance(raw_state, list):
            return []

        try:
            return messages_from_dict(raw_state)
        except Exception:
            return []

    def add_message(self, message: BaseMessage) -> None:
        """Append a message to the agent's persistent chat history."""
        curr_messages = self.messages
        curr_messages.append(message)
        serialized = messages_to_dict(curr_messages)
        self._agent.state.set(self.session_key, serialized)

    def add_messages(self, messages: Sequence[BaseMessage]) -> None:
        """Append multiple messages to the agent's persistent chat history."""
        curr_messages = self.messages
        curr_messages.extend(messages)
        serialized = messages_to_dict(curr_messages)
        self._agent.state.set(self.session_key, serialized)

    def clear(self) -> None:
        """Clear the agent's persistent chat history."""
        self._agent.state.delete(self.session_key)


# Convenience alias for persistent conversational memory
AetherDBMemory = AetherDBChatMessageHistory
