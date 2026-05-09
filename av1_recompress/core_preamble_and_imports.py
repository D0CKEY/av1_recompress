"""
================================================================================
AV1 BATCH ENCODER - FULL OPERATION DOCUMENTATION
================================================================================

OVERVIEW
========
This application performs batch video transcoding to AV1 format, supporting:
- NVENC (NVIDIA GPU) and SVT-AV1 (CPU) encoders
- Parallel processing (multi-worker architecture)
- VMAF/PSNR quality measurement
- Automatic CQ/CRF optimization
- Audio track manipulation
- Database-based state saving

ARCHITECTURE COMPONENTS
=========================

1. MAIN THREADS AND QUEUES
   -----------------------
   
   a) Main Thread (GUI Thread):
      - Running Tkinter GUI
      - Handling user interactions
      - Executing GUI updates (check_encoding_queue)
      - Coordinating database operations
      
   b) Encoding Coordinator Thread:
      - encoding_worker() function
      - Selecting videos to encode
      - Filling NVENC/SVT queues
      - Collecting results
      - Automatic CQ adjustment based on VMAF
      
   c) NVENC Worker Threads (1-3 pcs):
      - nvenc_worker(worker_index) function
      - Processing NVENC queue
      - GPU-based encoding (parallel)
      - Using ab-av1 for CQ determination
      - Validation with VirtualDub2 (frame count)
      
   d) SVT-AV1 Worker Thread (1 pc):
      - svt_worker() function
      - Processing SVT queue
      - CPU-based encoding (single)
      - Validation with VirtualDub2
      - Fallback copy if encoding fails
      
   e) VMAF/PSNR Worker Thread (1 pc):
      - vmaf_worker() function
      - Processing VMAF_QUEUE
      - Quality measurements (ab-av1 or ffmpeg-libvmaf)
      - CPU-intensive, so only 1 runs at a time
      
   f) Audio Edit Worker Thread:
      - audio_edit_worker() function
      - Processing AUDIO_EDIT_QUEUE
      - Removing/converting audio tracks
      
   g) Manual NVENC Thread:
      - process_manual_nvenc_tasks_worker()
      - Handling manual re-encoding
      - With dedicated CQ value
      
   h) Video Loading Threads (pool):
      - Loading videos in parallel
      - Extracting FFprobe data
      - Searching for subtitle files
      
   i) Database Save Thread:
      - Saving database in background
      - Using WAL mode
      - Batch insert optimization

   QUEUES:
   --------
   - encoding_queue: GUI updates (all workers -> GUI)
   - NVENC_QUEUE: NVENC tasks (coordinator -> NVENC workers)
   - SVT_QUEUE: SVT-AV1 tasks (coordinator -> SVT worker)
   - VMAF_QUEUE: VMAF/PSNR tasks (workers -> VMAF worker)
   - AUDIO_EDIT_QUEUE: Audio operation tasks (GUI -> audio worker)

   LOCKS:
   --------
   - ACTIVE_PROCESSES_LOCK: For list of running subprocesses
   - VMAF_LOCK: For VMAF/PSNR worker coordination
   - CPU_WORKER_LOCK: For limiting shared CPU usage between SVT and VMAF
   - db_lock: Synchronizing database operations


================================================================================
DATABASE OPERATION AND PROBING STRATEGY
================================================================================

1. COLD START (load_videos function)
   ====================================
   
   DEFINITION:
   - No existing database entry for videos
   - Loading videos from a folder for the first time
   
   PROCESS:
   a) Processing video files:
      - PROBING happens for every video file (ffprobe):
        * source_duration_seconds (duration in seconds)
        * source_fps (frames per second)
        * source_frame_count (total frame count)
      - Checking output files (if they exist):
        * output_encoder_type (nvenc/svt-av1) - From Settings tag
        * output_file_size_bytes (file size)
        * output_modified_timestamp (modification date)
      - Source file stat() call:
        * orig_size_bytes (source file size)
        * source_modified_timestamp (modification date)
   
   b) Data enters the Tree:
      - All probed data appears in the GUI tree
      - Duration, frames, file size, status, etc.
   
   c) DB save in background thread AFTER loading:
      - save_state_to_db() is called in background thread
      - IMPORTANT: We DO NOT probe again in save_state_to_db during cold start!
      - OPTIMIZATION: First collect all tree data into a cache (faster than one by one)
      - Using original data behind Tree item (fast, without parsing):
        * First try original data behind tree item (self.tree_item_data[item_id])
        * source_duration_seconds, source_frame_count, source_fps -> directly, without parsing!
        * CQ, VMAF, PSNR -> directly (float values), without parsing!
        * new_size_bytes -> directly (int value), without parsing!
        * output_encoder_type -> directly, without probing!
        * Fallback: only parse from tree if no data behind tree item
      - Using cached stat() values (already stat()-ed during loading):
        * Use source_size_bytes cached during loading (no parsing, no re-stat()!)
        * Use source_modified_timestamp cached during loading (if available)
        * IMPORTANT: Do not call stat() again if already called during loading!
        * Fallback: only stat() if no cache (rare case, e.g. new video or error occurred)
      - Checking output files:
        * Cold start: DO NOT check output files (exists(), stat()), because we already checked during loading!
        * Cold start: DO NOT probe (output_encoder_type comes from data behind tree item)
        * Warm start: only check if DB entry exists (file might have changed)
      - Batch INSERT into database (1000 videos per batch)
      - WAL checkpoint at the end (deleting journal file)
   
   OPTIMIZATION:
   - Probing only happens during loading, not during DB save
   - Storing original data behind Tree item: store original data (source_duration_seconds, source_frame_count, source_fps, output_encoder_type) in self.tree_item_data[item_id] during loading
   - Updating data behind Tree item: update tree_item_data after every operation (encoding, VMAF measurement):
     * CQ, VMAF, PSNR values (float) - without parsing
     * new_size_bytes (int) - parsed from new_size string
     * output_encoder_type (string) - probed if completed status
   - Caching Tree data: first collect all tree data into a cache (faster than one by one)
   - Using data behind Tree item: use original data behind tree item first during cold start (fast, without parsing!)
     * source_duration_seconds, source_frame_count, source_fps -> directly
     * CQ, VMAF, PSNR -> directly (float values)
     * new_size_bytes -> directly (int value)
     * output_encoder_type -> directly
   - Fallback: only parse from tree if no data behind tree item (e.g. new video, or error occurred)
   - Output file checking skipped during cold start (already checked during loading)


2. WARM START (load_videos function)
   ====================================
   
   DEFINITION:
   - Existing database entry for videos
   - We have loaded and saved video data before
   
   PROCESS:
   a) Loading from database:
      - load_state_from_db() loads previous data
      - Source video data (frame_count, duration, fps, size, timestamp)
      - Output video data (encoder_type, size, timestamp)
      - Status, CQ, VMAF, PSNR values
   
   b) File change check:
      - stat() call for every video (file size, modification date)
      - Comparison with values saved in DB:
        * If file size differs (orig_size_bytes != stat().st_size) -> PROBING
        * If modification date differs (>1 second) -> PROBING
      - If NOT changed -> Use data from DB, DO NOT probe
   
   c) Output file check (if completed status):
      - stat() call (file size, modification date)
      - Comparison with values saved in DB:
        * If file size or date differs -> PROBING (encoder_type)
        * If NOT changed -> Use encoder_type from DB
   
   OPTIMIZATION:
   - Only probe if file ACTUALLY changed
   - Update file size and timestamp from stat() result
   - Use probed data from DB if file not changed


3. START BUTTON (start_encoding function)
   =====================================
   
   PROCESS:
   a) Copying non-video files:
      - Copying images, text files etc. to destination folder
      - Asynchronously, with progress bar
   
   b) Database save (save_state_to_db):
      - Checks if DB save is already running (after cold start)
      - If running, waits for it (max 5 minutes timeout)
      - Uses warm start logic:
        * DB entry exists -> only probe if file changed
        * No DB entry -> reads data from tree
      - Batch INSERT (1000 videos per batch)
      - WAL checkpoint at the end
   
   c) Starting encoding worker:
      - Starting NVENC and SVT-AV1 worker threads
      - Filling queues with waiting videos
      - Starting encoding process
   
   OPTIMIZATION:
   - After Start button, same logic as warm start
   - Only probe if file actually changed
   - Read data from tree during cold start


4. save_state_to_db PROBING STRATEGY
   =====================================
   
   SOURCE VIDEO:
   ------------
   a) Cold start (no DB entry):
      - OPTIMIZATION: First use original data behind tree item (fast, without parsing!)
        * source_duration_seconds, source_frame_count, source_fps -> directly from self.tree_item_data[item_id]
        * CQ, VMAF, PSNR -> directly from self.tree_item_data[item_id] (float values)
        * new_size_bytes -> directly from self.tree_item_data[item_id] (int value)
      - Fallback: only parse from tree if no data behind tree item
      - Only probe if no data in tree (e.g. new video, error occurred)
      - Using cached stat() values (already stat()-ed during loading)
   
   b) Warm start (DB entry exists):
      - stat() call -> comparison with DB values
      - If changed (size or date) -> PROBING
      - If NOT changed -> Use data from DB
      - Update file size and timestamp from stat() result
   
   OUTPUT VIDEO (if completed status):
   -----------------------------------
   a) Cold start (no DB entry):
      - OPTIMIZATION: DO NOT check output files (exists(), stat()), because we already checked during loading!
      - OPTIMIZATION: DO NOT probe (output_encoder_type comes from data behind tree item: self.tree_item_data[item_id]['output_encoder_type'])
      - Fallback: only probe if no data behind tree item
   
   b) Warm start (DB entry exists):
      - stat() call -> comparison with DB values
      - If changed (size or date) -> PROBING (encoder_type)
      - If NOT changed -> Use encoder_type from DB
      - Update file size and timestamp from stat() result
   
   OPTIMIZATIONS:
   - Cold start: Store original data behind Tree item (during loading: self.tree_item_data[item_id])
   - After every operation: Update data behind Tree item (after encoding, VMAF measurement: CQ, VMAF, PSNR, new_size_bytes, output_encoder_type)
   - Cold start: Use data behind Tree item (fast, without parsing!)
   - Cold start: Query Tree item data once (optimization: original_data queried only once)
   - Cold start: Cache Tree data (collect all tree data first)
   - Cold start: Output file checking skipped (already checked during loading)
   - Cold start: Output encoder_type probing skipped (comes from data behind tree item)
   - Warm start: Only probe if file changed
   - PRAGMA settings before transaction (WAL mode, synchronous) - cannot be modified within transaction
   - Batch INSERT (1000 videos per batch) - faster than one by one
   - WAL mode and checkpoint - deleting journal file
   - Progress callback - progress visible (every 50 videos or 2 seconds)


================================================================================
ENCODING WORKFLOW
================================================================================

5. ENCODING COORDINATOR (encoding_worker)
   =======================================
   
   TASK:
   - Selects videos to encode (in order, "Pending" status)
   - Decides: NVENC or SVT-AV1*
   - Places tasks into queues
   - Collects and processes results
   - Performs VMAF-based CQ adjustment
   
   PROCESS:
   a) Video selection:
      - Iterates through tree by order number
      - Only takes "Pending" status videos
      - Checks: NVENC enabled* SVT enabled*
      - Auto mode: NVENC if GPU detected, otherwise SVT
      
   b) Placing into Queue:
      - NVENC_QUEUE.put(task) - For GPU tasks
      - SVT_QUEUE.put(task) - For CPU tasks
      - Task contains: video_path, item_id, target_cq/vmaf, settings
      
   c) Result processing:
      - Receives: success/fail, vmaf_result, output_path
      - If VMAF < target:
        * Decrease CQ (by vmaf_step)
        * Re-queue (max 10 attempts)
        * Fallback to SVT if NVENC cannot find suitable CQ
      - If VMAF >= target:
        * Set "Completed" status
        * Start VMAF/PSNR measurement (if auto_vmaf_psnr enabled)
      
   d) CPU worker coordination:
      - Using CPU_WORKER_LOCK
      - SVT and VMAF CANNOT run simultaneously (CPU)
      - Wait if CPU worker is active


6. NVENC WORKER (nvenc_worker)
   ============================
   
   TASK:
   - Processing NVENC_QUEUE
   - GPU-based AV1 encoding
   - ab-av1 CQ determination (if auto mode)
   - Validation with VirtualDub2
   
   PROCESS:
   a) Queue monitoring:
      - NVENC_QUEUE.get(timeout=1)
      - Waiting for new task or stop signal
      
   b) CQ determination:
      - If "auto" mode: running ab-av1 crf-search
        * --min-vmaf parameter
        * --max-encoded-percent parameter
        * Sample encode (fast estimation)
        * Result: optimal CQ value
      - If "manual" mode: using pre-defined CQ
      
   c) Encoding:
      - Calling encode_single_attempt()
      - FFmpeg + av1_nvenc encoder
      - Embedding subtitles (if any)
      - Audio compression (if enabled)
      - Resizing (if enabled)
      - Writing Settings metadata
      
   d) Validation:
      - VirtualDub2 frame export (1 frame)
      - Frame count check (ffprobe)
      - If differs >5%: "Check" status
      - If OK: back to coordinator
      
   e) Error handling:
      - EncodingStopped -> immediate interruption
      - NoSuitableCRFFound -> SVT fallback
      - Other error -> "Error" status, retry


7. SVT-AV1 WORKER (svt_worker)
   ===========================
   
   TASK:
   - Processing SVT_QUEUE
   - CPU-based AV1 encoding
   - Validation with VirtualDub2
   
   PROCESS (similar to NVENC):
   a) Queue monitoring: SVT_QUEUE.get(timeout=1)
   b) CQ determination: ab-av1 or manual CRF
   c) Encoding: FFmpeg + libsvtav1 encoder
   d) Validation: ffprobe frame count
   e) Result: back to coordinator
   
   DIFFERENCES from NVENC:
   - Using CPU_WORKER_LOCK (shared with VMAF)
   - Only 1 instance can run (CPU limitation)
   - Slower, but universal (no GPU needed)
   - Preset setting (0-13, default: 2)


8. VMAF/PSNR CALCULATION (vmaf_worker)
   =================================
   
   TASK:
   - Processing VMAF_QUEUE
   - Quality measurements on completed videos
   - Updating metadata
   
   PROCESS:
   a) Queue monitoring:
      - VMAF_QUEUE.get(timeout=5)
      - Only completed videos (completed status)
      
   b) Deciding measurement mode:
      - ab-av1 preferred (faster):
        * ab-av1 vmaf --reference --distorted
        * Progress bar from ab-av1 output
        * VMAF + xPSNR together
      - Fallback: ffmpeg-libvmaf
        * ffmpeg -lavfi libvmaf
        * Slower, but universal
      
   c) Running measurement:
      - Progress callback: updates GUI
      - Interpolation: 1% updates (if ab-av1 doesn't give frequent updates)
      - Stop check: STOP_EVENT.is_set()
      
   d) Result processing:
      - VMAF value into Settings metadata
      - PSNR value into Settings metadata
      - Updating Tree: vmaf, psnr columns
      - Updating tree_item_data
      
   e) CPU coordination:
      - Using CPU_WORKER_LOCK
      - Shared CPU usage with SVT
      - Only 1 CPU-intensive task at a time


================================================================================
GUI AND UPDATE MECHANISM
================================================================================

9. GUI UPDATES (check_encoding_queue)
   =======================================
   
   TASK:
   - Processing encoding_queue
   - Executing GUI updates (thread-safe)
   - Handling different types of messages
   
   MESSAGE TYPES:
   
   a) ("nvenc_log", worker_idx, logger_idx, text):
      - NVENC worker console output
      - Selecting appropriate tab (based on logger_idx)
      - Inserting text into console
      
   b) ("svt_log", text):
      - SVT-AV1 worker console output
      - Writing to SVT tab
      
   c) ("update", item_id, status, cq, vmaf, psnr, progress, ...):
      - Updating Tree item
      - Status, metrics, progress columns
      - Updating tree_item_data cache
      - Hiding completed item (if enabled)
      
   d) ("progress", item_id, progress_text):
      - Updating Progress column
      - Calculating estimated completion time
      
   e) ("status_only", item_id, status_text):
      - Updating only status (progress unchanged)
      
   f) ("tag", item_id, tag_name):
      - Setting Tree item tag
      - Coloring: completed (green), error (red), pending (yellow)
      
   g) ("debug_pause", ...):
      - Debug mode: stopping for user input
      - Waiting for Continue event
      
   h) ("save_json",):
      - DEPRECATED: no longer used
      - Previously triggered JSON save
      
   i) ("db_progress", message):
      - Database save progress
      - Updating status label
   
   UPDATE FREQUENCY:
   - Check every 100ms (self.root.after(100, ...))
   - Batch processing of multiple messages
   - Automatic scroll (autoscroll enabled)


10. STOP MECHANISMS
    ===================
    
    TWO TYPES OF STOP:
    
    a) Graceful Stop (stop_encoding_graceful):
       - graceful_stop_requested = True
       - Finishing current video
       - NO new videos started
       - Emptying queues
       - Workers exit on their own
       - Saving status to database
       
    b) Immediate Stop (stop_encoding_immediate):
       - STOP_EVENT.set()
       - All subprocess.terminate()
       - Worker threads exit immediately
       - Queues not emptied
       - Resetting partial results to "Pending"
       
    COORDINATION:
    - STOP_EVENT: threading.Event (global)
    - ACTIVE_PROCESSES: list of running subprocesses
    - ACTIVE_PROCESSES_LOCK: thread-safe access
    
    CLEANUP:
    - Deleting ab-av1 temp directories
    - Flushing log files
    - Database checkpoint (WAL)


================================================================================
AUDIO OPERATIONS
================================================================================

11. AUDIO PROCESSING (audio_edit_worker)
    =====================================
    
    TASK:
    - Processing AUDIO_EDIT_QUEUE
    - Removing audio tracks
    - 5.1 -> 2.0 conversion
    
    TYPES:
    
    a) Audio track removal:
       - Using FFmpeg -map
       - Skipping selected audio stream
       - Keeping other streams (video, subtitles, other audio)
       - Updating metadata
       
    b) 5.1 -> 2.0 conversion:
       - Two methods:
         * "fast": pan filter (fast, simple downmix)
         * "dialogue": atempo + volume boost (dialogue focus)
       - Adding NEW 2.0 track (keeping original 5.1)
       - Copying language code
       - Setting Title ("2.0 Stereo")
       
    PROCESS:
    - Renaming original file (.original)
    - Creating new file (FFmpeg)
    - Successful -> deleting original
    - Failed -> restoring original
    - Metadata update (Settings tag)


================================================================================
SUBTITLE MANAGEMENT
================================================================================

12. SUBTITLE MANAGEMENT
    ====================
    
    PROCESS:
    
    a) Searching for subtitle files:
       - find_subtitle_files(video_path)
       - Based on video name (.srt, .ass, .ssa, .vtt, .sub)
       - Language code detection from filename:
         * video.hu.srt -> "hu" language
         * video-eng.srt -> "eng" language
         * video.srt -> no language
       
    b) Validation:
       - is_valid_subtitle_file()
       - File size check (>10 bytes)
       - Format specific regexes:
         * SRT: timecode pattern
         * VTT: WEBVTT header
         * ASS/SSA: [Events] section
         * SUB: MicroDVD frame pattern
       
    c) Language normalization:
       - ISO 639-1/639-2 codes
       - Fallback: "und" (undefined)
       
    d) Embedding during encoding:
       - FFmpeg -i input.srt
       - -metadata:s:s:N language=hun
       - Into MKV container
       
    e) Copying:
       - Copying valid subtitles alongside output
       - Invalid subtitles separately (invalid_reasons)


================================================================================
ERROR HANDLING AND EXCEPTIONS
================================================================================

13. ERROR HANDLING
    ===============
    
    EXCEPTION CLASSES:
    
    a) EncodingStopped:
       - STOP_EVENT detected
       - Normal stop (not error)
       - Exit without cleanup
       
    b) NoSuitableCRFFound:
       - ab-av1 did not find suitable CQ
       - Fallback: SVT-AV1 attempt
       - Or "Check" status
       
    c) NVENCFallbackRequired:
       - NVENC specific error
       - Automatic SVT fallback
       
    ERROR STATUSES:
    - "Error" - general encoding error
    - "Check" - suspicious result (frame count mismatch)
    - "VMAF Error" - VMAF calculation failed
    - "Playback Error" - VirtualDub2 could not open
    
    RETRY MECHANISM:
    - CQ adjustment: max 10 attempts
    - Automatic SVT fallback after NVENC error
    - Storing partial results


================================================================================
OPTIMIZATIONS AND PERFORMANCE
================================================================================

14. PERFORMANCE OPTIMIZATIONS
    ==========================
    
    a) Parallel processing:
       - Multi-worker NVENC (1-3 GPU tasks in parallel)
       - Dedicated SVT worker (CPU)
       - Dedicated VMAF worker (CPU)
       - Video loading pool (I/O parallelization)
       
    b) Database optimizations:
       - WAL mode (Write-Ahead Logging)
       - Batch INSERT (1000/batch)
       - Background thread save (does not block GUI)
       - Tree data cache (avoiding parsing)
       
    c) FFprobe cache:
       - Cold start: probing 1x during loading
       - Storing Tree item data (avoiding parsing)
       - Stat cache (file size/mtime)
       - Warm start: only if file changed
       
    d) GUI updates:
       - Debouncing (not every message separately)
       - Batch processing (multiple items at once)
       - Autoscroll optimization
       - Progress interpolation (UI responsiveness)
       
    e) Subprocess management:
       - Startup info (hiding Windows console)
       - Process tracking (ACTIVE_PROCESSES)
       - Timeout handling
       - Priority setting (LOW_PRIORITY)


================================================================================
CONFIGURATION SETTINGS
================================================================================

15. CONFIGURATION PARAMETERS
    =========================
    
    a) Encoding parameters:
       - min_vmaf: Minimum VMAF target (e.g. 95)
       - vmaf_step: VMAF step size for CQ adjustment (e.g. 0.5)
       - max_encoded_percent: Max file size % (e.g. 75%)
       - resize_enabled: Enable resizing
       - resize_height: Target height (e.g. 1080)
       
    b) Audio settings:
       - audio_compression_enabled: Audio compression
       - audio_compression_method: "fast" or "dialogue"
       - auto_51_to_stereo: Automatic 5.1->2.0 (DEPRECATED)
       
    c) Worker settings:
       - nvenc_worker_count: NVENC worker count (1-3)
       - nvenc_enabled: Use NVENC
       - svt_enabled: Use SVT-AV1
       - svt_preset: SVT preset (0-13, default: 2)
       
    d) VMAF/PSNR:
       - auto_vmaf_psnr: Automatic measurement after encoding
       - Use ab-av1 for VMAF: Prefer ab-av1 over ffmpeg
       
    e) UI settings:
       - hide_completed: Hide completed videos
       - autoscroll: Automatic scrolling in console
       - language: 'hu' or 'en'
       
    f) Debug:
       - DEBUG_MODE: Enable debug pauses
       - LOAD_DEBUG: Loading debug log
       - VIDEO_LOADING_DEBUG: Detailed video loading log


================================================================================
IMPORTANT NOTES
================================================================================

16. IMPORTANT NOTES
    ================
    
    - During cold start, probing happens during LOADING, not during DB save
    - In save_state_to_db, we read data from tree during cold start
    - During warm start, we only probe if file actually changed
    - Stat() calls always happen (file size, timestamp)
    - Batch INSERT is faster than one by one (1000 videos per batch)
    - WAL checkpoint ensures journal file is deleted
    - Progress callback called frequently to show progress
    - CPU_WORKER_LOCK guarantees: SVT and VMAF DO NOT run simultaneously
    - STOP_EVENT ensures thread-safe stopping
    - tree_item_data cache reduces number of parse/probe operations
    - Console logging is thread-safe (ConsoleLogger + STDOUT_ROUTER)
    - Database operations are locked (db_lock)
    - VirtualDub2 is only needed for NVENC (frame export validation)
    - ab-av1 is optional (fallback: manual CQ, ffmpeg-libvmaf)

================================================================================
"""

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
import json  # Required for FFprobe JSON output
from datetime import datetime
import locale
import multiprocessing
import traceback
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Windows CPU priority setting
if platform.system() == 'Windows':
    try:
        PROCESS_SET_INFORMATION = 0x0200
        PROCESS_QUERY_INFORMATION = 0x0400
        BELOW_NORMAL_PRIORITY_CLASS = 0x4000
        
        kernel32 = ctypes.windll.kernel32
        
        def set_low_priority():
            """Sets the CPU priority of the current process to low on Windows."""
            handle = kernel32.OpenProcess(PROCESS_SET_INFORMATION | PROCESS_QUERY_INFORMATION, False, os.getpid())
            if handle:
                kernel32.SetPriorityClass(handle, BELOW_NORMAL_PRIORITY_CLASS)
                kernel32.CloseHandle(handle)
    except (OSError, AttributeError, ctypes.WinError):
        def set_low_priority():
            """Fallback - does nothing if it fails."""
            pass
else:
    def set_low_priority():
        """Does nothing on non-Windows systems."""
        pass

# Helper function to terminate child processes
def terminate_process_tree(process):
    """Terminate a process and all its children.
    
    Recursively terminates the specified process and all its child processes
    using psutil logic (or taskkill on Windows).
    
    Args:
        process: A subprocess.Popen object or psutil.Process object, or None.
    """
    if not process:
        return
    try:
        if platform.system() == 'Windows':
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(process.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        else:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except (OSError, subprocess.SubprocessError, ProcessLookupError, AttributeError) as e:
        # If taskkill/killpg fails, try terminating directly
        try:
            if process and hasattr(process, 'terminate'):
                process.terminate()
        except (OSError, ProcessLookupError, AttributeError) as e2:
            # If terminate also fails, silently swallow the error
            # (the process might have already stopped)
            pass

# Supported extensions
VIDEO_EXTENSIONS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
    '.mpg', '.mpeg', '.m2ts', '.mts', '.ts', '.vob', '.3gp', '.3g2',
    '.ogv', '.ogm', '.divx', '.xvid', '.rm', '.rmvb', '.asf'
}
SUBTITLE_EXTENSIONS = {'.srt', '.vtt', '.sub', '.ass', '.ssa'}

# Global language variable
CURRENT_LANGUAGE = 'hu'  # 'hu' or 'en'

# Frame/size/duration check tolerances
FRAME_MISMATCH_RATIO = 0.005  # Tolerant up to 0.5% deviation
FRAME_MISMATCH_MIN_DIFF = 5   # Minimum 5 frames difference required for alert
SIZE_MISMATCH_RATIO = 0.12    # Final size smaller than 12% is suspicious
DURATION_MISMATCH_RATIO = 0.95  # Length <95% is suspicious

# Frame validation constants
MAX_MEAN_BRIGHTNESS = 20  # For black frame detection
MIN_STD_DEV = 5.0  # For black frame detection
MIN_FILE_SIZE_BYTES = 10000  # Minimum file size for validation
MIN_FRAME_FILE_SIZE = 1000  # Minimum frame file size (byte)

# Global variables for logging and process management
LOG_WRITER = None
VIDEO_LOADING_LOG = None
VIDEO_LOADING_DEBUG = False
LOAD_DEBUG = False

class DebugMode:
    """Dynamic handle for debug mode to ensure instant propagation across modules.
    
    Thread-safe implementation using RLock.
    """
    def __init__(self, initial=False):
        self._value = initial
        self._lock = threading.RLock()
        
    def __bool__(self):
        with self._lock:
            return self._value
            
    def set(self, val):
        with self._lock:
            self._value = bool(val)
            
    def get(self):
        with self._lock:
            return self._value
            
    def __repr__(self):
        with self._lock:
            return str(self._value)
            
    def __str__(self):
        with self._lock:
            return str(self._value)

DEBUG_MODE = DebugMode(False)
ACTIVE_PROCESSES = []
ACTIVE_PROCESSES_LOCK = threading.Lock()
STOP_EVENT = threading.Event()

GUI_INSTANCE = None
APP_ROOT = None

# Global Queues
NVENC_QUEUE = queue.Queue()
SVT_QUEUE = queue.Queue()
VMAF_QUEUE = queue.Queue()
AUDIO_EDIT_QUEUE = queue.Queue()

# Global Locks
CPU_WORKER_LOCK = threading.Lock()
VMAF_LOCK = threading.Lock()

# Default paths
FFMPEG_PATH = "ffmpeg"
FFPROBE_PATH = "ffprobe"
ABAV1_PATH = "ab-av1"
VDUB2_PATH = "VirtualDub2"
DEFAULT_FFMPEG = "ffmpeg"
DEFAULT_FFPROBE = "ffprobe"
DEFAULT_ABAV1 = "ab-av1"

# Paths that will be set properly in __main__ or after APP_ROOT is known
# Initial defaults
MKVMERGE_PATH = "mkvmerge"


def format_cmd_for_windows(cmd):
    """
    Formats a command list for Windows CMD display with proper quoting.
    
    Args:
        cmd: List of command arguments (e.g., ['ffmpeg', '-i', 'input file.mp4', ...])
    
    Returns:
        String: Properly quoted command for Windows CMD
    
    Example:
        >>> format_cmd_for_windows(['ffmpeg', '-i', 'my file.mp4', '-c:v', 'copy', 'output.mp4'])
        'ffmpeg -i "my file.mp4" -c:v copy "output.mp4"'
    """
    # On Windows, shlex.quote doesn't work properly, so we use custom logic
    def quote_arg(arg):
        """Quote a single argument if it contains spaces or special characters."""
        arg_str = str(arg)
        
        # If already quoted, return as-is
        if arg_str.startswith('"') and arg_str.endswith('"'):
            return arg_str
        
        # Characters that require quoting
        needs_quoting = ' ' in arg_str or any(char in arg_str for char in ['&', '|', '<', '>', '^', '(', ')', '%', '!'])
        
        if needs_quoting:
            # Escape existing quotes
            escaped = arg_str.replace('"', '\\"')
            return f'"{escaped}"'
        
        return arg_str
    
    return ' '.join(quote_arg(arg) for arg in cmd)
