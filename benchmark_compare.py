import os
import shutil
import time
import random
import string

from kvstore.log_kv import LogStructuredKV
from kvstore.file_kv import FilePerKeyKV

# All benchmark data goes under this base directory
BASE_DATA_DIR = "data"


def prepare_data_dir(subfolder: str) -> str:
    """
    Ensure a clean directory under BASE_DATA_DIR/subfolder.
    """
    path = os.path.join(BASE_DATA_DIR, subfolder)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)
    return path


# ----------------- Helpers ------------------

def random_string(length: int = 100) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


def measure(func, num_ops: int):
    """
    Run func() and measure elapsed time + throughput.
    """
    start = time.time()
    func()
    end = time.time()
    elapsed = end - start
    tps = num_ops / elapsed if elapsed > 0 else float("inf")
    return elapsed, tps


def benchmark(db_cls, data_dir: str, num_ops: int, value_size: int, do_delete: bool = True):
    """
    Run PUT, GET, DELETE benchmark on a given DB class.

    Returns:
        (put_time, put_tps, get_time, get_tps, del_time, del_tps)
    """
    # Directory already cleaned by prepare_data_dir in main, so no need to delete here.

    db = db_cls(data_dir)

    # ---------- Benchmark PUT ----------
    def do_put():
        for i in range(num_ops):
            db.set(f"key_{i}", random_string(value_size))

    put_time, put_tps = measure(do_put, num_ops)

    # ---------- Benchmark GET ----------
    def do_get():
        for i in range(num_ops):
            db.get(f"key_{i}")

    get_time, get_tps = measure(do_get, num_ops)

    # ---------- Benchmark DELETE ----------
    if do_delete:
        def do_delete_func():
            for i in range(num_ops):
                db.delete(f"key_{i}")

        del_time, del_tps = measure(do_delete_func, num_ops)
    else:
        del_time, del_tps = 0.0, 0.0

    db.close()
    return put_time, put_tps, get_time, get_tps, del_time, del_tps


# ----------------- Printing ------------------

def print_results(name: str, r, num_ops: int):
    put_t, put_s, get_t, get_s, del_t, del_s = r

    total_time = put_t + get_t + del_t
    total_ops = num_ops * 3  # PUT + GET + DELETE
    total_tps = total_ops / total_time if total_time > 0 else float("inf")

    print(f"\n----> {name}")
    print(f"PUT    : {put_t:.3f} sec, {put_s:.0f} operations/sec")
    print(f"GET    : {get_t:.3f} sec, {get_s:.0f} operations/sec")
    print(f"DELETE : {del_t:.3f} sec, {del_s:.0f} operations/sec")
    print(f"TOTAL  : {total_time:.3f} sec, {total_tps:.0f} operations/sec")


# ----------------- Main ------------------

def main():
    num_ops = 20_000
    value_size = 100

    FS_DO_DELETE = True

    print(f"\nBenchmarking {num_ops} operations (value size={value_size} bytes)\n")

    # Optimized engine under data/log
    log_dir = prepare_data_dir("log")
    optimized_result = benchmark(LogStructuredKV, log_dir, num_ops, value_size, do_delete=True)
    print_results("Optimized LogStructuredKV (append + index)", optimized_result, num_ops)

    # Filesystem baseline under data/fs
    fs_dir = prepare_data_dir("fs")
    basic_result = benchmark(FilePerKeyKV, fs_dir, num_ops, value_size, do_delete=FS_DO_DELETE)
    print_results("Filesystem (file-based KV store)", basic_result, num_ops)

    # Speedup summary
    print("\n==== Speedup (Optimized vs Filesystem) ====")

    # Per-phase speedup
    labels = ["PUT", "GET", "DELETE"]
    for i, label in enumerate(labels):
        opt_tps = optimized_result[i * 2 + 1]  # ops/sec
        base_tps = basic_result[i * 2 + 1]
        if base_tps > 0:
            print(f"{label}: {opt_tps / base_tps:.2f}x faster")
        else:
            print(f"{label}: INF (filesystem too slow)")

    # Total speedup
    opt_put_t, _, opt_get_t, _, opt_del_t, _ = optimized_result
    base_put_t, _, base_get_t, _, base_del_t, _ = basic_result

    opt_total_time = opt_put_t + opt_get_t + opt_del_t
    base_total_time = base_put_t + base_get_t + base_del_t

    opt_total_tps = (num_ops * 3) / opt_total_time if opt_total_time > 0 else float("inf")
    base_total_tps = (num_ops * 3) / base_total_time if base_total_time > 0 else float("inf")

    if base_total_tps > 0:
        print(f"TOTAL: {opt_total_tps / base_total_tps:.2f}x faster")
    else:
        print("TOTAL: INF (filesystem too slow)")

    print()


if __name__ == "__main__":
    main()
