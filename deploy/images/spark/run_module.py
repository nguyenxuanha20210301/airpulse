"""entry point for the spark operator, runs a package module by name.

the operator needs a file path as the main application, while every job is a module with a
main function, so this file bridges the two.
"""

import runpy
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage, run_module.py <module> [args]")
    module = sys.argv[1]
    sys.argv = [module] + sys.argv[2:]
    runpy.run_module(module, run_name="__main__", alter_sys=True)
