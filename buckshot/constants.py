"""
Package constants.
"""

from __future__ import absolute_import

import multiprocessing


CPU_COUNT = multiprocessing.cpu_count()  # Number of CPUs on the system.
TASK_TIMEOUT = 60 * 60 * 12 # 12 hours