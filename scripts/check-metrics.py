import os
import re
import subprocess
from typing import Set, Dict

# todo: rewrite this script to use the rust-analyzer API or tree-sitter


def find_metrics(root_dir: str, blacklisted_dirs: [str]) -> Set[str]:
    metric_names = set()
    constants: Dict[str, str] = {}

    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".rs"):
                file_path = os.path.join(root, file)
                process_file(file_path, metric_names, constants, blacklisted_dirs)

    return metric_names


def process_file(
    file_path: str,
    metric_names: Set[str],
    constants: Dict[str, str],
    blacklisted_dirs: [str],
):
    with open(file_path, "r") as file:
        content = file.read()

    # Find and process constants
    const_pattern = r'const\s+([A-Z_]+)\s*:\s*&str\s*=\s*"([^"]+)";'
    for match in re.finditer(const_pattern, content):
        constants[match.group(1)] = match.group(2)

    # Find metrics macros and HistogramGuard::begin
    patterns = [
        r"metrics::gauge!\s*\(\s*([^)]+)\s*\)",
        r"metrics::histogram!\s*\(\s*([^)]+)\s*\)",
        r"metrics::counter!\s*\(\s*([^)]+)\s*\)",
        r"HistogramGuard::begin\s*\(\s*([^)]+)\s*\)",
        r'HistogramGuard::begin_with_labels\(\s*(?:([A-Z_]+)|("(?:[^"\\]|\\.)*"))',
    ]
    patterns = [re.compile(pattern, re.MULTILINE) for pattern in patterns]

    lines = content.splitlines()
    for pattern in patterns:
        for match in re.finditer(pattern, content):
            arg = next(group for group in match.groups() if group is not None).strip()
            line_number = content[: match.start()].count("\n") + 1
            line = lines[line_number - 1].strip()

            if not line.startswith("//"):  # Skip commented lines
                process_metric_arg(
                    arg, metric_names, constants, file_path, blacklisted_dirs
                )


def process_metric_arg(
    arg: str,
    metric_names: Set[str],
    constants: Dict[str, str],
    file_path: str,
    blacklisted_dirs: [str],
):
    arg = arg.split(",")[0].strip()  # Remove labels
    if arg.startswith('"') and arg.endswith('"'):
        # It's a string literal
        metric_names.add(arg[1:-1])
        return  # No need to check further
    if arg in constants:
        # It's a constant, resolve it
        metric_names.add(constants[arg])
    elif "::" in arg:
        # It might be a constant from another module
        parts = arg.split("::")
        constant_name = parts[-1]
        constant_value = find_constant_in_imports(file_path, constant_name)
        if constant_value:
            metric_names.add(constant_value)
        else:
            if not any(dir in file_path for dir in blacklisted_dirs):
                print(f"Warning: Unresolved metric name '{arg}' in {file_path}")
    else:
        # It might be a constant from another file or an unresolved variable
        constant_value = find_constant_in_imports(file_path, arg)
        if constant_value:
            metric_names.add(constant_value)
        else:
            if not any(dir in file_path for dir in blacklisted_dirs):
                print(f"Warning: Unresolved metric name '{arg}' in {file_path}")


def find_constant_in_imports(file_path: str, constant_name: str) -> str:
    with open(file_path, "r") as file:
        content = file.read()

    # Find use statements
    use_pattern = r"use\s+([^;]+);"
    for match in re.finditer(use_pattern, content):
        module_path = match.group(1)
        if "::" in module_path:
            module_parts = module_path.split("::")
            potential_file = os.path.join(
                os.path.dirname(file_path), *module_parts[:-1], f"{module_parts[-1]}.rs"
            )
            if os.path.exists(potential_file):
                with open(potential_file, "r") as imported_file:
                    imported_content = imported_file.read()
                    const_pattern = (
                        rf'const\s+{constant_name}\s*:\s*&str\s*=\s*"([^"]+)";'
                    )
                    const_match = re.search(const_pattern, imported_content)
                    if const_match:
                        return const_match.group(1)

    return ""


if __name__ == "__main__":
    root_directory = (
        subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True)
        .stdout.decode()
        .strip()
    )
    blacklisted_dirs = [
        "network/connection_manager.rs",
        "network/src/overlay/metrics.rs",
        "src/util/alloc.rs",
        "util/src/metrics",
        "util/src/sync/rayon.rs",
    ]
    metrics = find_metrics(root_directory, blacklisted_dirs)
    dashboard_data = open(f"{root_directory}/scripts/gen-dashboard.py", "r").read()
    exit_code = 0
    for metric in sorted(metrics):
        if metric not in dashboard_data:
            exit_code = 1
            print(f"Missing metric: {metric}")

    exit(exit_code)
