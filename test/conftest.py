import time

import pytest
import os
import signal
import sys
import subprocess
import multiprocessing
import time


def pytest_addoption(parser):
    parser.addoption('--dist_exec', action='store', default='build/zmq_distributor')
    parser.addoption('--work_exec', action='store', default='build/zmq_worker')
    parser.addoption('--debug_test', action='store_true', default=False)
    parser.addoption('--no_cache', action='store_true', default=False)
    parser.addoption('--cache_dir', action='store', default="")

# def pytest_unconfigure():
#     print("Unconfigured pytest")
