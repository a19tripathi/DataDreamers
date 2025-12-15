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