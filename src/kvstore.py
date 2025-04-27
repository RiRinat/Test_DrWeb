import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from src.logs import TransactionLogger


class KVStore:
    def __init__(self):
        self._data: Dict[str, str] = {} # key -> value
        self._val_keys: Dict[str, Set[str]] = defaultdict(set)  # value -> set of keys
        self._tx_stack: List[Tuple[Dict[str, str], Dict[str, Set[str]], TransactionLogger]] = []  # Стек: (data_snapshot, val_keys_snapshot, logger)
        self._current_logger: Optional[TransactionLogger] = None

    def __repr__(self):
        return f"KVStore(data={self._data}, val_keys={dict(self._val_keys)})"

    def _snapshot(self) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
        """Создает объект текущего состояния."""
        return (self._data.copy(), defaultdict(set, {k: v.copy() for k, v in self._val_keys.items()}))

    def _restore(self, snapshot: Tuple[Dict[str, str], Dict[str, Set[str]]]):
        """Восстанавливает состояние из объекта _snapshot."""
        self._data, self._val_keys = snapshot

    def begin(self):
        """Начинает новую транзакцию."""
        logger = TransactionLogger()
        self._tx_stack.append((self._snapshot(), logger))
        self._current_logger = logger
        logger.log("BEGIN")

    def rollback(self) -> bool:
        """Откатывает текущую транзакцию. Возвращает False, если нет активной транзакции."""
        if not self._tx_stack:
            print("NO TRANSACTION")
            return False

        snapshot, logger = self._tx_stack.pop()
        self._restore(snapshot)
        print(f"ROLLBACK: Changes reverted. Log: {logger.get_changes()}")
        self._current_logger = self._tx_stack[-1][1] if self._tx_stack else None
        return True

    def commit(self) -> bool:
        """Фиксирует текущую транзакцию. Возвращает False, если нет активной транзакции."""
        if not self._tx_stack:
            print("NO TRANSACTION")
            return False

        _, logger = self._tx_stack.pop()
        print(f"COMMIT: Changes applied. Log: {logger.get_changes()}")
        self._current_logger = self._tx_stack[-1][1] if self._tx_stack else None
        return True

    def set(self, key: str, value: str):
        """Устанавливает значение ключа. Логирует изменение, если внутри транзакции."""
        # Удаляем старое значение (если было)
        if key in self._data:
            old_val = self._data[key]
            self._val_keys[old_val].discard(key)
            if not self._val_keys[old_val]:
                del self._val_keys[old_val]

        # Устанавливаем новое значение
        self._data[key] = value
        self._val_keys[value].add(key)

        # Логируем изменение
        if self._current_logger:
            self._current_logger.log(f"SET {key} {value}")

    def unset(self, key: str):
        """Удаляет ключ. Логирует изменение, если внутри транзакции."""
        if key in self._data:
            old_val = self._data.pop(key)
            self._val_keys[old_val].discard(key)
            if not self._val_keys[old_val]:
                del self._val_keys[old_val]

            if self._current_logger:
                self._current_logger.log(f"UNSET {key}")

    def get(self, key: str) -> str:
        """Возвращает значение ключа или 'NULL'."""
        return self._data.get(key, "NULL")

    def counts(self, value: str) -> int:
        """Возвращает количество ключей с указанным значением."""
        return len(self._val_keys.get(value, set()))

    def find(self, value: str) -> List[str]:
        """Возвращает список ключей с указанным значением."""
        keys = self._val_keys.get(value, set())
        return sorted(keys) if keys else []

    def process_command(self, parts: List[str]):
        """Обрабатывает введенную команду."""
        if not parts:
            return

        cmd = parts[0].upper()
        command_map = {
            'SET': lambda: self._handle_set(parts),
            'GET': lambda: print(self.get(parts[1])),
            'UNSET': lambda: self.unset(parts[1]),
            'COUNTS': lambda: print(self.counts(parts[1])),
            'FIND': lambda: self._handle_find(parts),
            'BEGIN': lambda: self.begin(),
            'ROLLBACK': lambda: self.rollback(),
            'COMMIT': lambda: self.commit(),
            'END': lambda: sys.exit(0)
        }

        try:
            if cmd in command_map:
                command_map[cmd]()
            else:
                print(f"INVALID COMMAND: {' '.join(parts)}")
        except (IndexError, Exception) as e:
            print(f"ERROR: {str(e)}")

    def _handle_set(self, parts):
        if len(parts) == 3:
            self.set(parts[1], parts[2])
        else:
            print("INVALID SET COMMAND")

    def _handle_find(self, parts):
        if len(parts) == 2:
            keys = self.find(parts[1])
            print(' '.join(keys) if keys else "NONE")
        else:
            print("INVALID FIND COMMAND")