import os
import subprocess
import sys
import re
import shutil
import random
import tempfile
import platform
import signal
from pathlib import Path

from PIL import Image
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import scrolledtext
from contextlib import contextmanager
import ctypes
import sqlite3
import json  # FFprobe JSON kimenetéhez szükséges
from datetime import datetime
import locale
import multiprocessing
import traceback
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Common imports used by mixins
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Import i18n functions
from .i18n import t, format_localized_number
