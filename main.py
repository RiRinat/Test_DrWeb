from src.kvstore import KVStore


def main():
    store = KVStore()
    print("Enter commands. Press Ctrl+D (Unix) or Ctrl+Z (Windows) to exit.")
    while True:
        try:
            line = input("> ").strip()
            if not line:
                continue
            parts = line.split()
            store.process_command(parts)
        except EOFError:
            print("\nExiting.")
            break
        except KeyboardInterrupt:
            print("\nExiting.")
            break


if __name__ == '__main__':
    main()