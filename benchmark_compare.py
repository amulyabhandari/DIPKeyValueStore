import os
import shutil
import time
import random
import string

from kvstore.log_kv import LogStructuredKV
from kvstore.file_kv import FilePerKeyKV


# ----------------- Helpers ------------------

def random_string(length=100):
    alphabet = string.ascii_letters + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


def measure(label, func, num_ops):
    start = time.time()
    func()
    end = time.time()
    elapsed = end - start
    tps = num_ops / elapsed if elapsed > 0 else float("inf")
    return elapsed, tps


def benchmark(db_cls, data_dir, num_ops, value_size):
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    db = db_cls(data_dir)

    # ---------- Benchmark PUT ----------
    def do_put():
        for i in range(num_ops):
            db.set(f"key_{i}", random_string(value_size))

    put_time, put_tps = measure("PUT", do_put, num_ops)

    # ---------- Benchmark GET ----------
    def do_get():
        for i in range(num_ops):
            db.get(f"key_{i}")

    get_time, get_tps = measure("GET", do_get, num_ops)

    # ---------- Benchmark DELETE ----------
    def do_delete():
        for i in range(num_ops):
            db.delete(f"key_{i}")

    delete_time, delete_tps = measure("DELETE", do_delete, num_ops)

    db.close()
    return put_time, put_tps, get_time, get_tps, delete_time, delete_tps


# ----------------- Main ------------------

def print_results(name, r):
    put_t, put_s, get_t, get_s, del_t, del_s = r
    print(f"\n----> {name}")
    print(f"PUT    : {put_t:.3f} sec, {put_s:.0f} operations/sec")
    print(f"GET    : {get_t:.3f} sec, {get_s:.0f} operations/sec")
    print(f"DELETE : {del_t:.3f} sec, {del_s:.0f} operations/sec")


def main():
    num_ops = 20_000
    value_size = 100

    print(f"\nBenchmarking {num_ops} operations (value size={value_size} bytes)\n")

    # Optimized
    optimized_result = benchmark(LogStructuredKV, "data_log", num_ops, value_size)
    print_results("Optimized LogStructuredKV (append + index)", optimized_result)

    # Filesystem
    basic_result = benchmark(FilePerKeyKV, "data_fs", num_ops, value_size)
    print_results("Filesystem (one file per key)", basic_result)

    # Speedup summary
    print("\n==== Speedup (Optimized vs Filesystem) ====")
    labels = ["PUT", "GET", "DELETE"]
    for i, label in enumerate(labels):
        opt = optimized_result[i * 2 + 1]
        base = basic_result[i * 2 + 1]
        if base > 0:
            print(f"{label}: {opt / base:.2f}x faster")
        else:
            print(f"{label}: INF (filesystem too slow)")

    print()


if __name__ == "__main__":
    main()