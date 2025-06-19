import uuid
import time
import logging
import os
from typing import Dict, List, Any, Optional
from queue import Queue
from dataclasses import dataclass
from openai import OpenAI
from openai.types.chat import ChatCompletion

logger = logging.getLogger(__name__)

@dataclass
class Message:
    """Message structure for agent communication."""
    id: str
    sender_id: str
    content: str
    metadata: Dict[str, Any] = None
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.metadata is None:
            self.metadata = {}

class BaseAgent:
    """
    Base agent class that provides OpenAI API integration and messaging capabilities.
    
    This class is designed to be extended by specific agent implementations.
    """
    
    def __init__(
        self, 
        agent_id: str = None,
        name: str = "BaseAgent",
        openai_api_key: str = None,
        model: str = "gpt-4o",
        temperature: float = 0.7,
        max_tokens: int = 1000
    ):
        # Agent identification
        self.agent_id = agent_id or str(uuid.uuid4())
        self.name = name
        
        # Message handling
        self.inbox: Queue = Queue()
        self.sent_messages: List[Message] = []
        self.received_messages: List[Message] = []
        
        # OpenAI API setup
        if openai_api_key is None:
            openai_api_key = os.getenv('OPENAI_API_KEY', None)

        self.openai_api_key = openai_api_key
        self.client = None
        if openai_api_key:
            self.client = OpenAI(api_key=openai_api_key)
        
        # Default model parameters
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        
        # Agent state
        self.memory: Dict[str, Any] = {}
        self.is_active = True
        
        logger.info(f"Agent {self.name} ({self.agent_id}) initialized")

    def setup_openai(self, api_key: str) -> None:
        """Set up OpenAI client with the provided API key."""
        self.openai_api_key = api_key
        self.client = OpenAI(api_key=api_key)
        logger.info(f"OpenAI client configured for agent {self.name}")
    
    def send_message(self, recipient_agent: 'BaseAgent', content: str, metadata: Dict[str, Any] = None) -> Message:
        """Send a message to another agent's inbox."""
        message = Message(
            id=str(uuid.uuid4()),
            sender_id=self.agent_id,
            content=content,
            metadata=metadata or {}
        )
        
        recipient_agent.receive_message(message)
        self.sent_messages.append(message)
        logger.debug(f"Agent {self.name} sent message to {recipient_agent.name}: {content[:50]}...")
        
        return message
    
    def receive_message(self, message: Message) -> None:
        """Receive a message and add it to the inbox queue."""
        self.inbox.put(message)
        self.received_messages.append(message)
        logger.debug(f"Agent {self.name} received message from {message.sender_id}: {message.content[:50]}...")
    
    def check_messages(self) -> List[Message]:
        """Check and retrieve all messages in the inbox."""
        messages = []
        while not self.inbox.empty():
            messages.append(self.inbox.get())
        return messages
    
    def get_next_message(self) -> Optional[Message]:
        """Get the next message from the inbox if available."""
        if not self.inbox.empty():
            return self.inbox.get()
        return None
    
    def chat_completion(
        self, 
        messages: List[Dict[str, str]],
        model: str = None,
        temperature: float = None,
        max_tokens: int = None
    ) -> ChatCompletion:
        """
        Send a request to OpenAI chat completion API.
        
        Args:
            messages: List of message dictionaries with role and content
            model: Optional model override
            temperature: Optional temperature override
            max_tokens: Optional max tokens override
            
        Returns:
            OpenAI ChatCompletion object
        """
        if not self.client:
            raise ValueError("OpenAI client not configured. Call setup_openai() first.")
        
        try:
            response = self.client.chat.completions.create(
                model=model or self.model,
                messages=messages,
                temperature=temperature or self.temperature,
                max_tokens=max_tokens or self.max_tokens
            )
            return response
        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            raise
    
    async def process_message(self, message: Message) -> Optional[str]:
        """
        Process a message and generate a response.
        
        This method should be overridden by specific agent implementations.
        """
        logger.warning(f"Base process_message called in {self.name}. This method should be overridden.")
        return f"Received message: {message.content}"
    
    async def run_cycle(self) -> None:
        """
        Run a single processing cycle for the agent.
        
        This method should be overridden by specific agent implementations.
        """
        # Process any messages in the inbox
        message = self.get_next_message()
        if message:
            response = await self.process_message(message)
            logger.info(f"Agent {self.name} processed message: {response[:50] if response else 'No response'}")
    
    async def run(self, cycles: int = None) -> None:
        """
        Run the agent for a specified number of cycles or indefinitely.
        
        Args:
            cycles: Number of cycles to run, or None to run indefinitely
        """
        cycle_count = 0
        
        while self.is_active:
            if cycles is not None and cycle_count >= cycles:
                break
                
            await self.run_cycle()
            cycle_count += 1
            
            # Small delay to prevent tight loops
            time.sleep(0.01)
    
    def stop(self) -> None:
        """Stop the agent from running."""
        self.is_active = False
        logger.info(f"Agent {self.name} stopped")