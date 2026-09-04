#!/usr/bin/env python3

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any
import sys
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import traceback
import os
import glob
import threading
import re
import multiprocessing as mp
from pathlib import Path
import shutil
import argparse
import coverage


class PythonCoverageMerger:
    def __init__(self, cov_dir: Path, consolidated_cov_dir: Path):
        self.cov_dir = cov_dir
        self.consolidated_cov_dir = consolidated_cov_dir

    def _latest_consolidated(self):
        covs = sorted(self.consolidated_cov_dir.glob("coverage_*.cov"))
        return covs[-1] if covs else None

    def merge_and_cleanup(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_cov = self.consolidated_cov_dir / f"coverage_{timestamp}.cov"

        cov = coverage.Coverage(data_file=str(output_cov), branch=True)

        prev_cov = self._latest_consolidated()
        if prev_cov is not None:
            cov.combine([str(prev_cov)], keep=True)

        raw_cov_files = list(self.cov_dir.glob(".coverage*"))
        if not raw_cov_files:
            return None

        cov.combine([str(p) for p in raw_cov_files])
        cov.save()

        for p in raw_cov_files:
            p.unlink(missing_ok=True)

        return output_cov


COV_DIR = None  # set at startup via --out-dir


def execute_in_process(code, result_queue, namespace, cov_dir):
    """Worker function that runs the generated program in its own subprocess,
    so a segfault/abort in pandas' native extensions can't take the server down."""

    if cov_dir:
        cov = coverage.Coverage(
            config_file="oracle-servers/pandas-oracle-server/.coveragerc",
            source=["pandas"],
            data_file=os.path.join(cov_dir, ".coverage"),
            data_suffix=True,
            concurrency="multiprocessing",
            branch=True,
        )
        cov.start()

    captured_output = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured_output

    try:
        exec(code, namespace, namespace)
        result_queue.put({
            "success": True,
            "error_name": "",
            "error_message": "",
            "stdout": captured_output.getvalue()
        })
    except Exception as e:
        result_queue.put({
            "success": False,
            "error_name": type(e).__name__,
            "error_message": str(e),
            "stdout": captured_output.getvalue()
        })
    finally:
        sys.stdout = old_stdout
        if cov_dir:
            cov.stop()
            cov.save()


class GlobalState:
    port = 8891
    program_counter = 0
    counter_lock = threading.Lock()
    log_dir = 'oracle-servers/.logs/pandas-server-log'

    table_schemas = None
    timeout_seconds = 30

    coverage_merger = None
    coverage_capture_on = True
    coverage_secs_since_last_merge = 0
    coverage_merge_interval_seconds = 60
    coverage_timestamp_of_last_merge = datetime.now()


GLOBAL_STATE = GlobalState()


class MultilineJSONEncoder(json.JSONEncoder):
    def encode(self, obj):
        result = super().encode(obj)

        def replace_newlines(match):
            return f'"{match.group(1).replace(chr(92) + "n", chr(10))}"'

        return re.sub(r'"([^"]*\\n[^"]*)"', replace_newlines, result)


class PandasFuzzingHandler(BaseHTTPRequestHandler):

    def setup_logging_directory(self, log_dir):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            print(f"[{datetime.now()}] Created logging directory: {log_dir}")
        else:
            print(f"[{datetime.now()}] Using existing logging directory: {log_dir}")

    def get_next_program_number(self):
        with GLOBAL_STATE.counter_lock:
            current_number = GLOBAL_STATE.program_counter
            GLOBAL_STATE.program_counter += 1
            return current_number

    def create_program_log_directory(self, program_number):
        program_dir = os.path.join(GLOBAL_STATE.log_dir, str(program_number))
        os.makedirs(program_dir, exist_ok=True)
        return program_dir

    def save_program_logs(self, program_number, received_code):
        try:
            program_dir = self.create_program_log_directory(program_number)
            with open(os.path.join(program_dir, 'program.py'), 'w', encoding='utf-8') as f:
                f.write(received_code)
            print(f"[{datetime.now()}] Saved logs for program {program_number} to {program_dir}")
        except Exception as e:
            print(f"[{datetime.now()}] Failed to save logs for program {program_number}: {e}")

    # ---------------- execution ----------------

    def _try_execute_code(self, code, namespace):
        """Run the program in a subprocess, catching timeouts/crashes as well as exceptions."""
        result_queue = mp.Queue()
        p = mp.Process(target=execute_in_process, args=(code, result_queue, namespace, COV_DIR))
        p.start()
        p.join(timeout=GLOBAL_STATE.timeout_seconds)

        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join()
            return {
                "success": False,
                "error_name": "TimeoutError",
                "error_message": f"Code execution timed out after {GLOBAL_STATE.timeout_seconds} seconds"
            }

        if p.exitcode != 0:
            if p.exitcode == -11:
                error_name, error_message = "SegmentationFault", "Process crashed with segmentation fault (SIGSEGV)"
            elif p.exitcode == -6:
                error_name, error_message = "Abort", "Process aborted (SIGABRT)"
            elif p.exitcode < 0:
                error_name, error_message = "ProcessCrash", f"Process killed by signal {-p.exitcode}"
            else:
                error_name, error_message = "ProcessError", f"Process exited with code {p.exitcode}"
            return {"success": False, "error_name": error_name, "error_message": error_message}

        try:
            return result_queue.get_nowait()
        except Exception:
            return {"success": False, "error_name": "UnknownError", "error_message": "No result received from subprocess"}

    def execute_pandas_code(self, code):
        try:
            result = self._try_execute_code(code, {})
        except Exception as e:
            return {
                "success": False,
                "error_name": type(e).__name__,
                "error_message": f"{e}\n{traceback.format_exc()}",
                "final_program": ""
            }

        result_name = "Success" if result["success"] else (result.get("error_name") or "UnknownError")
        details = {
            "result_name": result_name,
            "result_details": {k: v for k, v in result.items() if k != "stdout"}
        }
        final_program = (
            "# ======== Program ========\n"
            f"{code}\n\n"
            "# ======== Details ========\n"
            f'"""\n{json.dumps(details, indent=2, cls=MultilineJSONEncoder)}\n"""\n'
        )

        return {
            "success": result["success"],
            "error_name": result.get("error_name", ""),
            "error_message": result.get("error_message", ""),
            "final_program": final_program
        }

    def handle_execute_code(self, request_dict):
        print(f"[{datetime.now()}] Running handle_execute_code()...")
        code = request_dict['code']
        GLOBAL_STATE.coverage_secs_since_last_merge = (
            datetime.now() - GLOBAL_STATE.coverage_timestamp_of_last_merge
        ).total_seconds()

        try:
            program_number = self.get_next_program_number()
            print(f"\n{'='*60}\nPROGRAM #{program_number} RECEIVED\nLength: {len(code)} characters\n{'='*60}")
            print(code)
            print('=' * 60)

            print(f"[{datetime.now()}] Executing program #{program_number}...")
            execution_result = self.execute_pandas_code(code)
            self.save_program_logs(program_number, code)

            print(f"\n{'='*60}\nEXECUTION RESULTS FOR PROGRAM #{program_number}\nSuccess: {execution_result['success']}\n{'='*60}\n")

            if GLOBAL_STATE.coverage_capture_on and GLOBAL_STATE.coverage_secs_since_last_merge > GLOBAL_STATE.coverage_merge_interval_seconds:
                GLOBAL_STATE.coverage_timestamp_of_last_merge = datetime.now()
                GLOBAL_STATE.coverage_merger.merge_and_cleanup()
                GLOBAL_STATE.coverage_secs_since_last_merge = 0

            return execution_result
        except Exception as e:
            print(f"[{datetime.now()}] Error handling client {e}")
            return {"fatal_error": str(e), "stack_trace": traceback.format_exc()}

    # ---------------- schema / tables (identical shape to the Dask server) ----------------

    def get_tables(self):
        return self.convert_schema_format(GLOBAL_STATE.table_schemas)

    def handle_get_tables(self, request_dict):
        return {"tables": self.get_tables()}

    def load_tpcds_schema(self, json_file):
        with open(json_file, "r") as f:
            return json.load(f)

    def convert_schema_format(self, input_dict):
        result = []
        for table_name, columns in input_dict.items():
            table_schema = {"identifier": table_name, "columns": [], "metadata": {"source": "database", "owner": "admin"}}
            for column_def in columns:
                parts = column_def.split()
                if len(parts) < 2:
                    continue
                column_name = parts[0]
                data_type_raw = ' '.join(parts[1:])
                table_schema["columns"].append({
                    "name": column_name,
                    "dataType": self._parse_data_type(data_type_raw),
                    "isNullable": self._is_nullable_column(column_name),
                    "isKey": self._is_key_column(column_name)
                })
            result.append(table_schema)
        return result

    def _parse_data_type(self, type_str):
        type_str = type_str.upper()
        if 'INT' in type_str:
            return 'integer'
        if any(t in type_str for t in ['STRING', 'VARCHAR', 'CHAR']):
            return 'string'
        if any(t in type_str for t in ['DECIMAL', 'NUMERIC', 'FLOAT']):
            return 'decimal'
        if any(t in type_str for t in ['DATE', 'TIMESTAMP']):
            return 'date'
        if 'BOOL' in type_str:
            return 'boolean'
        return 'string'

    def _is_key_column(self, column_name):
        name_lower = column_name.lower()
        return any(p in name_lower for p in ['_id', '_sk', '_key', 'id_', 'key_']) or name_lower == 'id'

    def _is_nullable_column(self, column_name):
        if self._is_key_column(column_name):
            return False
        name_lower = column_name.lower()
        if any(p in name_lower for p in ['name', 'type', 'class', 'status']):
            return False
        return True

    def handle_load_data(self, request_dict):
        GLOBAL_STATE.port = 8891
        GLOBAL_STATE.program_counter = 0
        GLOBAL_STATE.counter_lock = threading.Lock()
        GLOBAL_STATE.log_dir = 'oracle-servers/.logs/pandas-server-log'

        self.setup_logging_directory(GLOBAL_STATE.log_dir)
        GLOBAL_STATE.table_schemas = self.load_tpcds_schema("oracle-servers/tpcds-schema.json")

        return {"success": True}

    # ---------------- HTTP plumbing (identical shape to the Dask server) ----------------

    def process_request(self, request_dict):
        message_type = request_dict.get('message_type')
        if message_type == 'get_tables':
            return self.handle_get_tables(request_dict)
        elif message_type == 'execute_code':
            return self.handle_execute_code(request_dict)
        elif message_type == 'load_data':
            return self.handle_load_data(request_dict)
        return {"error": f"Unknown message type: {message_type}"}

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            request_dict = json.loads(self.rfile.read(content_length).decode('utf-8'))
            response_dict = self.process_request(request_dict)
            response_json = json.dumps(response_dict, indent=2)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-length', str(len(response_json.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(response_json.encode('utf-8'))
        except Exception as e:
            error_json = json.dumps({"error": str(e)})
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-length', str(len(error_json.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(error_json.encode('utf-8'))


def run_server(port: int = 8891):
    server_address = ('localhost', port)
    httpd = HTTPServer(server_address, PandasFuzzingHandler)
    print(f"Starting server on http://localhost:{port}")
    print("Press Ctrl+C to stop the server")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


def get_coverage_out_dir_path(out_dir):
    coverage_path = Path(out_dir).parent / "coverage"
    consolidated_coverage_path = Path(out_dir).parent / "consolidated-coverage"
    if coverage_path.exists():
        shutil.rmtree(coverage_path)
    if consolidated_coverage_path.exists():
        shutil.rmtree(consolidated_coverage_path)
    coverage_path.mkdir(parents=True)
    consolidated_coverage_path.mkdir(parents=True)
    return coverage_path, consolidated_coverage_path


def setup_coverage_profiling(args):
    global COV_DIR
    COV_DIR, consolidated_cov_dir = get_coverage_out_dir_path(args.out_dir)
    os.makedirs(COV_DIR, exist_ok=True)
    GLOBAL_STATE.coverage_capture_on = args.coverage_capture
    for f in glob.glob(os.path.join(COV_DIR, ".coverage.*")):
        os.remove(f)
    GLOBAL_STATE.coverage_merger = PythonCoverageMerger(COV_DIR, consolidated_cov_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pandas oracle server")
    parser.add_argument('--out-dir', '-d', type=str, default='/tmp/coverage-pandas')
    parser.add_argument('--coverage-capture', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    if args.coverage_capture:
        setup_coverage_profiling(args)

    import signal

    def handle_shutdown(signum, frame):
        GLOBAL_STATE.coverage_merger.merge_and_cleanup()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)

    mp.set_start_method('spawn', force=True)
    run_server(port=GLOBAL_STATE.port)
