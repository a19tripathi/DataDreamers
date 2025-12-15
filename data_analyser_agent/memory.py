class AgentMemory:
    """Simple in-memory store for Agent state and results."""
    def __init__(self):
        self.store = {}

    def write(self, key: str, value: dict):
        self.store[key] = value

    def read(self, key: str):
        return self.store.get(key)

    def all(self):
        return self.store

# Create a global instance of the memory store.
# This instance will be shared across the application where this module is imported.
AGENT_MEMORY = AgentMemory()

# Expose the write method of the global instance as `memory_write`.
memory_write = AGENT_MEMORY.write