from typing import List

class TransactionLogger:
    """Логирует изменения внутри транзакции."""
    def __init__(self):
        self.changes: List[str] = []

    def log(self, message: str):
        self.changes.append(message)

    def get_changes(self) -> List[str]:
        return self.changes.copy()

    def clear(self):
        self.changes.clear()