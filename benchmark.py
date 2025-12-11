import os
import shutil
import time
import random
import string
import argparse

from kvstore.log_kv import LogStructuredKV




def random_string(length):
    """Generate a random ASCII string of given length."""
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


# ---------- Individual measurements ----------
def measure(label, func, num_ops):
    start = time.time()
    func()
    end = time.time()
    elapsed = end - start
    ops_per_sec = num_ops / elapsed if elapsed > 0 else float("inf")
    return elapsed, ops_per_sec


# ---------- Full benchmark on one DB ----------
def run_full_benchmark(data_dir, num_ops, value_size):
    # Start clean
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    db = LogStructuredKV(data_dir)

    # --------- PUT (set) ---------
    def do_put():
        for i in range(num_ops):
            db.set(f"key_{i}", random_string(value_size))

    put_time, put_ops = measure("PUT", do_put, num_ops)

    # --------- GET (read) ---------
    def do_get():
        for i in range(num_ops):
            db.get(f"key_{i}")

    get_time, get_ops = measure("GET", do_get, num_ops)

    # --------- DELETE ---------
    def do_delete():
        for i in range(num_ops):
            db.delete(f"key_{i}")

    del_time, del_ops = measure("DELETE", do_delete, num_ops)

    db.close()
    return put_time, put_ops, get_time, get_ops, del_time, del_ops


# ---------- MAIN ----------
def main():
    parser = argparse.ArgumentParser(description="Benchmark for LogStructuredKV")
    parser.add_argument(
        "--data-dir",
        default="data/benchmark",
        help="Directory to store log files for the benchmark (default: bench_data)",
    )
    parser.add_argument(
        "--num-ops",
        type=int,
        default=20_000,
        help="Number of operations per test (default: 20000)",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=100,
        help="Size of the random value (bytes) for each write (default: 100)",
    )

    args = parser.parse_args()

    print(f"\nBenchmarking {args.num_ops} operations")
    print(f"Value size: {args.value_size} bytes\n")

    # Run all benchmarks
    put_t, put_s, get_t, get_s, del_t, del_s = run_full_benchmark(
        args.data_dir, args.num_ops, args.value_size
    )

    # Print results
    print("----> Optimized LogStructuredKV")
    print(f"PUT    : {put_t:.3f} sec, {put_s:.0f} operations/sec")
    print(f"GET    : {get_t:.3f} sec, {get_s:.0f} operations/sec")
    print(f"DELETE : {del_t:.3f} sec, {del_s:.0f} operations/sec")
    print("\nBenchmark completed.\n")


if __name__ == "__main__":
    main()