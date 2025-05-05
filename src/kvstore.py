import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from src.logs import TransactionLogger


class KVStore:
    def __init__(self):
        """
        Инициализация key-value хранилища.
        """
        # Основное хранилище данных: ключ -> значение
        self._data: Dict[str, str] = {}

        # Обратный индекс: значение -> множество ключей с этим значением
        # Используем defaultdict для автоматического создания новых множеств
        self._val_keys: Dict[str, Set[str]] = defaultdict(set)

        # Стек для хранения снимков состояния при начале транзакций:
        # Каждый элемент - кортеж из (снимок данных, снимок обратного индекса, логгер)
        self._tx_stack: List[Tuple[Dict[str, str], Dict[str, Set[str]], TransactionLogger]] = []

        # Текущий активный логгер транзакций (None, если нет активной транзакции)
        self._current_logger: Optional[TransactionLogger] = None

    def __repr__(self) -> str:
        """
        Строковое представление объекта для отладки.
        Возвращает текущее состояние данных и обратного индекса.
        """
        return f"KVStore(data={self._data}, val_keys={dict(self._val_keys)})"

    def _snapshot(self) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
        """
        Создает глубокую копию текущего состояния хранилища.
        Возвращает:
            Кортеж из (копия _data, копия _val_keys)
        """
        # Копируем основной словарь и создаем новый defaultdict с копиями множеств
        return (
            self._data.copy(),
            defaultdict(set, {k: v.copy() for k, v in self._val_keys.items()})
        )

    def _restore(self, snapshot: Tuple[Dict[str, str], Dict[str, Set[str]]]) -> None:
        """
        Восстанавливает состояние хранилища из объекта _snapshot.
        Аргументы:
            snapshot: кортеж объектов _snapshot данных и обратного индекса
        """
        self._data, self._val_keys = snapshot

    def begin(self) -> None:
        """
        Начинает новую транзакцию.
        - Создает новый логгер
        - Сохраняет снимок текущего состояния
        - Помещает снимок и логгер в стек транзакций
        - Устанавливает текущий логгер
        """
        logger = TransactionLogger()  # Создаем новый логгер
        self._tx_stack.append((self._snapshot(), logger))  # Сохраняем объект состояния
        self._current_logger = logger  # Устанавливаем текущий логгер
        logger.log("BEGIN")  # Логируем начало транзакции

    def rollback(self) -> bool:
        """
        Откатывает текущую транзакцию.
        Возвращает:
            True - если откат выполнен
            False - если нет активных транзакций
        """
        if not self._tx_stack:
            print("NO TRANSACTION")  # Нет активных транзакций
            return False

        # Извлекаем последний объект состояния и логгер из стека
        snapshot, logger = self._tx_stack.pop()

        # Восстанавливаем состояние из последнего объекта состояния
        self._restore(snapshot)

        # Выводим информацию об откате
        print(f"ROLLBACK: Changes reverted. Log: {logger.get_changes()}")

        # Обновляем текущий логгер (берем из предыдущей транзакции, если есть)
        self._current_logger = self._tx_stack[-1][1] if self._tx_stack else None

        return True

    def commit(self) -> bool:
        """
        Фиксирует текущую транзакцию.
        Возвращает:
            True - если коммит выполнен
            False - если нет активных транзакций
        """
        if not self._tx_stack:
            print("NO TRANSACTION")  # Нет активных транзакций
            return False

        # Извлекаем логгер из стека (данные не нужны, так как изменения сохраняются)
        _, logger = self._tx_stack.pop()

        # Выводим информацию о коммите
        print(f"COMMIT: Changes applied. Log: {logger.get_changes()}")

        # Обновляем текущий логгер
        self._current_logger = self._tx_stack[-1][1] if self._tx_stack else None

        return True

    def set(self, key: str, value: str) -> None:
        """
        Устанавливает значение для ключа.
        Если ключ уже существует, обновляет его значение и обратный индекс.
        Логирует операцию, если выполняется внутри транзакции.
        """
        # Если ключ уже существует
        if key in self._data:
            old_val = self._data[key]
            # Удаляем ключ из старого значения в обратном индексе
            self._val_keys[old_val].discard(key)
            # Если больше нет ключей с этим значением, удаляем запись
            if not self._val_keys[old_val]:
                del self._val_keys[old_val]

        # Устанавливаем новое значение
        self._data[key] = value
        # Добавляем ключ в обратный индекс для нового значения
        self._val_keys[value].add(key)

        # Логируем операцию, если есть активная транзакция
        if self._current_logger:
            self._current_logger.log(f"SET {key} {value}")

    def unset(self, key: str) -> None:
        """
        Удаляет ключ из хранилища.
        Логирует операцию, если выполняется внутри транзакции.
        """
        if key in self._data:
            # Удаляем ключ и получаем его значение
            old_val = self._data.pop(key)
            # Удаляем ключ из обратного индекса
            self._val_keys[old_val].discard(key)
            # Если больше нет ключей с этим значением, удаляем запись
            if not self._val_keys[old_val]:
                del self._val_keys[old_val]

            # Логируем операцию, если есть активная транзакция
            if self._current_logger:
                self._current_logger.log(f"UNSET {key}")

    def get(self, key: str) -> str:
        """
        Возвращает значение для ключа.
        Если ключ не существует, возвращает "NULL".
        """
        return self._data.get(key, "NULL")

    def counts(self, value: str) -> int:
        """
        Возвращает количество ключей с указанным значением.
        Использует обратный индекс для быстрого поиска.
        """
        return len(self._val_keys.get(value, set()))

    def find(self, value: str) -> List[str]:
        """
        Возвращает отсортированный список ключей с указанным значением.
        Если ключей нет, возвращает пустой список.
        """
        keys = self._val_keys.get(value, set())
        return sorted(keys) if keys else []

    def process_command(self, parts: List[str]) -> None:
        """
        Обрабатывает введенную команду.
        Аргументы:
            parts: список строк команды (разделенные по пробелам)
        """
        if not parts:  # Пустая команда
            return

        # Приводим команду к верхнему регистру
        cmd = parts[0].upper()

        # Словарь соответствия команд методам
        command_map = {
            'SET': lambda: self._handle_set(parts),
            'GET': lambda: print(self.get(parts[1])),
            'UNSET': lambda: self.unset(parts[1]),
            'COUNTS': lambda: print(self.counts(parts[1])),
            'FIND': lambda: self._handle_find(parts),
            'BEGIN': lambda: self.begin(),
            'ROLLBACK': lambda: self.rollback(),
            'COMMIT': lambda: self.commit(),
            'END': lambda: sys.exit(0)  # Завершение программы
        }

        try:
            if cmd in command_map:
                command_map[cmd]()  # Вызываем соответствующий метод
            else:
                print(f"INVALID COMMAND: {' '.join(parts)}")
        except (IndexError, Exception) as e:
            print(f"ERROR: {str(e)}")  # Обработка ошибок выполнения

    def _handle_set(self, parts: List[str]) -> None:
        """
        Обработчик команды SET.
        Проверяет корректность аргументов перед вызовом set().
        """
        if len(parts) == 3:  # SET key value
            self.set(parts[1], parts[2])
        else:
            print("INVALID SET COMMAND")  # Неправильное количество аргументов

    def _handle_find(self, parts: List[str]) -> None:
        """
        Обработчик команды FIND.
        Проверяет корректность аргументов перед вызовом find().
        """
        if len(parts) == 2:  # FIND value
            keys = self.find(parts[1])
            # Выводим ключи через пробел или "NONE", если ключей нет
            print(' '.join(keys) if keys else "NONE")
        else:
            print("INVALID FIND COMMAND")  # Неправильное количество аргументов