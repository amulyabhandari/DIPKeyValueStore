# main.py

import sys
import argparse
from kvstore.log_kv import LogStructuredKV


# ---------- MENU MODE (when no CLI args) ----------

def menu_mode():
    db = LogStructuredKV("data")

    try:
        while True:
            print("\n====== Key-Value Store ======")
            print("1) Add data")
            print("2) See data")
            print("3) Delete data")
            print("4) Compact logs")
            print("5) Exit")
            print("=============================")

            choice = input("Choose an option (1-5): ").strip()

            if choice == "1":
                key = input("Enter key: ")
                value = input("Enter value: ")
                db.set(key, value)
                print(f"Added: {key} -> {value}")

            elif choice == "2":
                key = input("Enter key to view: ")
                value = db.get(key)
                if value:
                    print(f"Value: {value.decode()}")
                else:
                    print("Key not found.")

            elif choice == "3":
                key = input("Enter key to delete: ")
                db.delete(key)
                print(f"🗑️ Deleted key: {key}")

            elif choice == "4":
                db.compact()
                print("🧹 Compaction done.")

            elif choice == "5":
                print("Goodbye!")
                break

            else:
                print("Invalid option! Try again.")
    finally:
        db.close()


# ---------- CLI MODE (when there ARE args) ----------

def cmd_set(args):
    db = LogStructuredKV(args.data_dir)
    try:
        db.set(args.key, args.value)
        print(f"OK: set {args.key!r} -> {args.value!r}")
    finally:
        db.close()


def cmd_get(args):
    db = LogStructuredKV(args.data_dir)
    try:
        value = db.get(args.key)
        if value is None:
            print("NOT FOUND")
        else:
            print(value.decode("utf-8"))
    finally:
        db.close()


def cmd_delete(args):
    db = LogStructuredKV(args.data_dir)
    try:
        db.delete(args.key)
        print(f"OK: deleted {args.key!r}")
    finally:
        db.close()


def cmd_compact(args):
    db = LogStructuredKV(args.data_dir)
    try:
        db.compact()
        print("OK: compaction completed")
    finally:
        db.close()


def cmd_repl(args):
    db = LogStructuredKV(args.data_dir)
    try:
        print("Interactive KV store. Type 'help' for commands, 'exit' to quit.")
        while True:
            raw = input("> ").strip()
            if not raw:
                continue
            if raw in ("exit", "quit"):
                break
            if raw == "help":
                print("Commands:")
                print("  set <key> <value>")
                print("  get <key>")
                print("  delete <key>")
                print("  compact")
                print("  exit")
                continue

            parts = raw.split()
            cmd = parts[0].lower()

            if cmd == "set" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                db.set(key, value)
                print("OK")
            elif cmd == "get" and len(parts) == 2:
                key = parts[1]
                value = db.get(key)
                if value is None:
                    print("NOT FOUND")
                else:
                    print(value.decode("utf-8"))
            elif cmd == "delete" and len(parts) == 2:
                key = parts[1]
                db.delete(key)
                print("OK")
            elif cmd == "compact":
                db.compact()
                print("OK: compacted")
            else:
                print("Invalid command. Type 'help'.")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Log-structured key/value store CLI"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory for storing log files (default: ./data)",
    )

    subparsers = parser.add_subparsers(dest="command")

    # set
    p_set = subparsers.add_parser("set", help="Set a key to a value")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.set_defaults(func=cmd_set)

    # get
    p_get = subparsers.add_parser("get", help="Get a value by key")
    p_get.add_argument("key")
    p_get.set_defaults(func=cmd_get)

    # delete
    p_del = subparsers.add_parser("delete", help="Delete a key")
    p_del.add_argument("key")
    p_del.set_defaults(func=cmd_delete)

    # compact
    p_compact = subparsers.add_parser("compact", help="Compact log segments")
    p_compact.set_defaults(func=cmd_compact)

    # repl (interactive shell)
    p_repl = subparsers.add_parser("repl", help="Interactive shell")
    p_repl.set_defaults(func=cmd_repl)

    return parser


def main():
    # if no extra args -> use menu mode
    if len(sys.argv) == 1:
        menu_mode()
        return

    # else: parse CLI subcommands
    parser = build_parser()
    args = parser.parse_args()

    if getattr(args, "command", None) is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()
