def run_crf_search(input_path, encoder='av1_nvenc', initial_min_vmaf=None, vmaf_step=None, max_encoded_percent=None, progress_callback=None, logger=None, stop_event=None, svt_preset=2, crf_increment=1, denoise_enabled=False):
    """Run CRF search using ab-av1 to find optimal encoding settings.
    
    Args:
        input_path: Path to input video.
        encoder: Encoder to use ('av1_nvenc' or 'svt-av1').
        initial_min_vmaf: Target VMAF score.
        vmaf_step: Step size for VMAF adjustment.
        max_encoded_percent: Maximum allowed size percentage of source.
        progress_callback: Callback for progress updates.
        logger: Logger instance.
        stop_event: Event to stop search.
        svt_preset: SVT-AV1 preset value.
        crf_increment: CRF step size for ab-av1 search (default 1).
        denoise_enabled: Whether denoising was applied (affects encoder params).
        
    Returns:
        tuple: (crf_value, vmaf_value, predicted_size_mb) where predicted_size_mb
               is the predicted output file size in MB (or None if not available).
               For NVENC fallback, returns (crf_value, vmaf_value, True) where True
               indicates fallback is needed.
    """
    import builtins as _builtins

    def _routed_print(*args, **kwargs):
        """Route prints to worker logger when available."""
        if logger is not None:
            sep = kwargs.get('sep', ' ')
            end = kwargs.get('end', '\n')
            msg = sep.join(str(arg) for arg in args) + end
            try:
                logger.write(msg)
                if kwargs.get('flush', False):
                    logger.flush()
            except Exception:
                _builtins.print(*args, **kwargs)
        else:
            _builtins.print(*args, **kwargs)

    # Keep function-local print isolated from global stdout fallback mixing.
    print = _routed_print

    # Check if file exists and is the correct file
    if not input_path.exists():
        raise FileNotFoundError(f"CRF search input file does not exist: {input_path}")
    
    # Check if ab-av1.exe exists
    if not Path(ABAV1_PATH).exists() and not shutil.which(ABAV1_PATH):
        error_msg = f"FATAL ERROR: ab-av1.exe not found! Path: {ABAV1_PATH}\n\nThe program cannot start ab-av1.exe, so CRF search is not possible.\n\nCheck if ab-av1.exe exists at the specified path, or set the correct path in settings."
        print(f"\n{'='*80}")
        print(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]")
        print(f"{'='*80}")
        print(error_msg)
        print(f"{'='*80}\n")
        if logger:
            try:
                logger.write(f"\n{'='*80}\n")
                logger.write(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]\n")
                logger.write(f"{'='*80}\n")
                logger.write(f"{error_msg}\n")
                logger.write(f"{'='*80}\n\n")
                logger.flush()
            except Exception:
                pass
        log_writer = get_log_writer()
        if log_writer:
            try:
                log_writer.write(f"\n{'='*80}\n")
                log_writer.write(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]\n")
                log_writer.write(f"{'='*80}\n")
                log_writer.write(f"{error_msg}\n")
                log_writer.write(f"{'='*80}\n\n")
                log_writer.flush()
            except Exception:
                pass
        raise FileNotFoundError(error_msg)
    
    # CRITICAL PROTECTION: Save absolute path and file identifiers BEFORE CRF search
    # This protects against mixing CRF values between different videos
    input_absolute_start = input_path.absolute()
    input_str = os.fspath(input_absolute_start)
    
    # CRITICAL: Save file identifiers (size, modification date) - protection against file replacement
    try:
        input_stat_start = input_path.stat()
        input_size_start = input_stat_start.st_size
        input_mtime_start = input_stat_start.st_mtime
    except (OSError, PermissionError) as e:
        raise FileNotFoundError(f"CRF search: failed to stat file: {input_path} - {e}")
    
    # Detailed logging: verify we are using the EXACT file
    print(f"[SCAN] STARTING CRF SEARCH:")
    print(f"   File name: {input_path.name}")
    print(f"   Absolute path: {input_str}")
    print(f"   File size: {input_size_start:,} bytes")
    print(f"   Modified: {datetime.fromtimestamp(input_mtime_start).strftime('%Y-%m-%d %H:%M:%S')}")
    
    initial_min_vmaf, vmaf_step, max_encoded_percent = resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent)

    if vmaf_step <= 0:
        vmaf_step = 2.5
    # FIX #6: Clamp minimum step to 0.5 — prevents ~infinite loop with tiny values
    if vmaf_step < 0.5:
        print(f"[WARN] VMAF step too small ({vmaf_step}), clamped to minimum 0.5")
        vmaf_step = 0.5

    min_vmaf = initial_min_vmaf
    min_vmaf_threshold = 85.0

    # SAFETY GUARD: Maximum iteration limit prevents unbounded loop if ab-av1
    # keeps returning non-zero exit codes without "Failed to find" text.
    # With default settings (start=95, step=0.5, threshold=85) this allows up
    # to 30 iterations. Lower bound ensures at least 5 attempts regardless of params.
    max_iterations = max(5, int((initial_min_vmaf - min_vmaf_threshold) / vmaf_step) + 5)
    iteration_count = 0

    if stop_event is None:
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        raise EncodingStopped()
    
    if progress_callback:
        encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
        min_vmaf_str = format_localized_number(min_vmaf, decimals=2)
        progress_callback(f"{encoder_label} CRF keresés (VMAF: {min_vmaf_str})")
    
    while min_vmaf >= min_vmaf_threshold:
        if stop_event.is_set():
            raise EncodingStopped()

        # SAFETY GUARD: Prevent unbounded iteration if ab-av1 keeps failing
        # without emitting the expected "Failed to find" error text.
        iteration_count += 1
        if iteration_count > max_iterations:
            print(f"[WARN] CRF search reached maximum iterations ({max_iterations}), aborting")
            break

        # IMPORTANT: We use absolute path to ensure we use the correct file
        # cwd=input_path.parent only sets the working directory, but -i parameter has absolute path
        if encoder == 'svt-av1':
            # 8K+ resolution requires preset >= 5 (M5) for SVT-AV1
            from .core_audio_video_ops import get_video_resolution
            _width_8k, _height_8k = get_video_resolution(input_path)
            if _width_8k and _height_8k and (_width_8k >= 7680 or _height_8k >= 4320) and svt_preset < 5:
                print(f"[INFO] 8K videó detektálva ({_width_8k}x{_height_8k}), SVT-AV1 preset emelve: {svt_preset} -> 5 (M5 minimum)")
                svt_preset = 5

            # SVT-AV1 encoder parameters depend on whether denoising was applied
            # User decision: Use same high-quality parameters for normal re-encoding as for denoising
            # Previous logic:
            # if denoise_enabled: svt_params = 'tune=0:aq-mode=2:enable-qm=1:qm-min=0:enable-dlf=0'
            # else: svt_params = 'tune=1'

            # New logic: Always use the high-quality param set
            svt_params = 'tune=0:aq-mode=2:enable-qm=1:qm-min=0:enable-dlf=0'

            ab_av1_cmd = [
                ABAV1_PATH, 'crf-search', '-i', input_str, 
                '-e', 'svt-av1', 
                '--min-vmaf', str(min_vmaf), 
                '--preset', str(svt_preset), 
                '--max-encoded-percent', f"{float(max_encoded_percent):.2f}", 
                '--max-crf', '60', 
                '--crf-increment', str(int(crf_increment)),
                '--svt', svt_params
            ]
        else:
            ab_av1_cmd = [ABAV1_PATH, 'crf-search', '-i', input_str, '-e', 'av1_nvenc', '--min-vmaf', str(min_vmaf), '--max-encoded-percent', f"{float(max_encoded_percent):.2f}", '--crf-increment', str(int(crf_increment))]
        
        if stop_event.is_set():
            raise EncodingStopped()
        
        try:
            print(f"\n{'='*80}")
            print(f"🎬 AB-AV1 CRF SEARCH ({encoder}) - Min VMAF: {min_vmaf}")
            print(f"{'='*80}")
            print(f"FILE: {input_path.name}")
            print(f"ABSOLUTE PATH: {input_str}")
            print(f"COMMAND: {format_cmd_for_windows(ab_av1_cmd)}")
            print(f"{'='*80}\n")

            # IMPORTANT: We do NOT set cwd because there might be identical filenames in different folders
            # Using absolute path ensures we use the correct file
            # Set FFPROBE_PATH environment variable so ab-av1 can find ffprobe
            env = os.environ.copy()
            if FFPROBE_PATH and Path(FFPROBE_PATH).exists():
                # Add ffprobe directory to PATH, or set FFPROBE_PATH if ab-av1 supports it
                ffprobe_dir = str(Path(FFPROBE_PATH).parent)
                if ffprobe_dir not in env.get('PATH', ''):
                    env['PATH'] = ffprobe_dir + os.pathsep + env.get('PATH', '')
            
            with subprocess.Popen(
                ab_av1_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
                startupinfo=get_startup_info(),
                env=env
            ) as process:
                
                # Process registration
                with ACTIVE_PROCESSES_LOCK:
                    ACTIVE_PROCESSES.append(process)
                
                full_output = []
                try:
                    for line in process.stdout:
                        print(line.rstrip())
                        full_output.append(line)
                except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
                    print(f"Process output reading error: {e}")
                
                try:
                    process.wait(timeout=3600)
                except subprocess.TimeoutExpired:
                    print(f"\n[WARN] AB-AV1 CRF SEARCH TIMEOUT (>1 óra) - Process killed")
                    process.kill()
                    process.wait()
                finally:
                    # Remove process from list
                    with ACTIVE_PROCESSES_LOCK:
                        try:
                            ACTIVE_PROCESSES.remove(process)
                        except ValueError:
                            pass
            
            full_output_text = ''.join(full_output)
            
            print(f"\n{'='*80}")
            print(f"AB-AV1 BEFEJEZVE - Return code: {process.returncode}")
            print(f"{'='*80}\n")
            
            if stop_event.is_set():
                raise EncodingStopped()
            
            # Helper function to convert size string (e.g. "4.36 GiB") to MB
            def parse_predicted_size_to_mb(size_str, unit_str):
                """Convert size string to MB."""
                try:
                    size_val = float(size_str)
                    unit_lower = unit_str.lower()
                    if 'gib' in unit_lower or 'gb' in unit_lower:
                        return size_val * 1024  # GiB/GB to MB
                    elif 'mib' in unit_lower or 'mb' in unit_lower:
                        return size_val  # Already MB
                    elif 'kib' in unit_lower or 'kb' in unit_lower:
                        return size_val / 1024  # KiB/KB to MB
                    else:
                        return size_val  # Assume MB if unknown
                except (ValueError, TypeError):
                    return None
            
            # Extended regex to capture size and unit (e.g. "4.36 GiB")
            crf_pattern = r'crf\s+(\d+)\s+VMAF\s+([\d.]+)\s+predicted\s+video\s+stream\s+size\s+([\d.]+)\s+(\w+)\s+\((\d+)%\)'
            all_crf_results = re.findall(crf_pattern, full_output_text)
            
            has_failed = 'Failed to find a suitable crf' in full_output_text or 'Error: Failed to find' in full_output_text
            is_svt_8k_preset_error = (
                encoder == 'svt-av1'
                and (
                    '8k+ resolution support is limited to M5 and faster presets' in full_output_text
                    or (
                        '8K and higher resolution support is currently a work-in-progress project' in full_output_text
                        and 'Error setting encoder parameters: bad parameter' in full_output_text
                    )
                )
            )

            if is_svt_8k_preset_error:
                if svt_preset < 5:
                    print(f"[WARN] SVT-AV1 8K preset hiba detektálva, preset emelése: {svt_preset} -> 5 (M5 minimum). Újrapróbálás...")
                    svt_preset = 5
                    cleanup_ab_av1_temp_dirs(input_path.parent)
                    continue  # Retry with higher preset
                else:
                    print("[WARN] SVT-AV1 8K hiba preset >= 5 mellett is. CRF keresés megszakítva.")
                    cleanup_ab_av1_temp_dirs(input_path.parent)
                    raise NoSuitableCRFFound("SVT-AV1 preset incompatible for 8K source (requires preset >= 5)")
            
            if has_failed:
                current_vmaf_str = format_localized_number(min_vmaf, decimals=2)
                next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=2)
                print(f"[WARN] Ab-av1 'Failed to find' error detected.")
                print(f"[WARN] VMAF reduction: {current_vmaf_str} -> {next_vmaf_str}")

                # NOTE: Do NOT cleanup temp dirs here - ab-av1 cache (.ab-av1-*) contains
                # sample encoding results that are reused in subsequent retries with lower VMAF.
                # Cleanup happens after successful CRF found or when loop ends (line 446+).

                # Decrement VMAF and continue loop
                min_vmaf -= vmaf_step
                if progress_callback:
                    encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                    progress_callback(f"{encoder_label} CRF keresés (VMAF fallback: {format_localized_number(min_vmaf, decimals=2)})")
                continue
            
            if process.returncode == 0:
                all_crf_vmaf_matches = re.findall(r'crf\s+(\d+(?:\.\d+)?)\s+VMAF\s+([\d.]+)', full_output_text)
                
                if all_crf_vmaf_matches:
                    last_match = all_crf_vmaf_matches[-1]
                    crf_value = float(last_match[0])
                    actual_vmaf = float(last_match[1])
                    
                    # Extract predicted size from last successful CRF line using the full pattern
                    predicted_size_mb = None
                    if all_crf_results:
                        # Find the matching CRF result with the same CRF value
                        for crf, vmaf, size_val, size_unit, size_pct in all_crf_results:
                            if int(crf) == int(crf_value):
                                predicted_size_mb = parse_predicted_size_to_mb(size_val, size_unit)
                                break
                    
                    print(f"[OK] CRF TALÁLVA: {crf_value} (VMAF: {actual_vmaf})")
                    if predicted_size_mb:
                        print(f"   Becsült méret: ~{format_localized_number(predicted_size_mb, decimals=1)} MB")
                    
                    debug_pause(
                        f"Ab-av1 CRF: {crf_value} (VMAF: {actual_vmaf})",
                        f"FFmpeg kódolás CRF {crf_value}-val",
                        f"Encoder: {encoder}"
                    )
                    
                    # CRITICAL PROTECTION: Check BEFORE returning CRF
                    if not input_path.exists():
                        raise FileNotFoundError(f"FATAL ERROR: Source file DISAPPEARED during CRF search!\n"
                                               f"File: {input_str}\n"
                                               f"This means the CRF value ({crf_value}) is invalid!")
                    
                    input_absolute_end = input_path.absolute()
                    if input_absolute_end != input_absolute_start:
                        raise ValueError(f"FATAL ERROR: Source file CHANGED during CRF search!\n"
                                       f"Start of CRF search: {input_absolute_start}\n"
                                       f"End of CRF search: {input_absolute_end}\n"
                                       f"This means the CRF value ({crf_value}) belongs to ANOTHER VIDEO!")
                    
                    try:
                        input_stat_end = input_path.stat()
                        if input_stat_end.st_size != input_size_start or abs(input_stat_end.st_mtime - input_mtime_start) > 1.0:
                            raise ValueError(f"FATAL ERROR: Source file MODIFIED during CRF search!\n"
                                           f"Size: {input_size_start:,} -> {input_stat_end.st_size:,} bytes\n"
                                           f"Date: {datetime.fromtimestamp(input_mtime_start).strftime('%Y-%m-%d %H:%M:%S')} -> {datetime.fromtimestamp(input_stat_end.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
                    except (OSError, PermissionError) as e:
                        raise FileNotFoundError(f"FATAL ERROR: Failed to verify file: {e}")

                    # NOTE: No cleanup here - ab-av1 cleans up its own temp dirs on successful exit.
                    # Cleanup on failure/timeout is handled by the while loop's exit paths (line 446+).
                    return (crf_value, actual_vmaf, predicted_size_mb)
                else:
                    all_crf_successful = re.findall(r'crf\s+(\d+(?:\.\d+)?)\s+successful', full_output_text)
                    if all_crf_successful:
                        crf_value = float(all_crf_successful[-1])
                        crf_value_str = format_localized_number(crf_value, decimals=1)
                        print(f"[OK] CRF TALÁLVA (utolsó successful): {crf_value_str}")
                        
                        # Try to get predicted size for this CRF
                        predicted_size_mb = None
                        if all_crf_results:
                            for crf, vmaf, size_val, size_unit, size_pct in all_crf_results:
                                if abs(float(crf) - crf_value) < 0.01:
                                    predicted_size_mb = parse_predicted_size_to_mb(size_val, size_unit)
                                    break
                        
                        debug_pause(
                            f"Ab-av1 CRF: {crf_value_str}",
                            f"FFmpeg kódolás",
                            f"Encoder: {encoder}"
                        )

                        # NOTE: No cleanup here - ab-av1 cleans up on successful exit.
                        return (crf_value, None, predicted_size_mb)
                    else:
                        current_vmaf_str = format_localized_number(min_vmaf, decimals=2)
                        next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=2)
                        print(f"[WARN] No CRF, VMAF reduction: {current_vmaf_str} -> {next_vmaf_str}")
                        min_vmaf -= vmaf_step
                        if progress_callback:
                            encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                            progress_callback(f"{encoder_label} CRF search (VMAF fallback: {format_localized_number(min_vmaf, decimals=2)})")
                        continue
            else:
                current_vmaf_str = format_localized_number(min_vmaf, decimals=2)
                next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=2)
                print(f"[WARN] Ab-av1 error (rc: {process.returncode}), VMAF reduction: {current_vmaf_str} -> {next_vmaf_str}")
                min_vmaf -= vmaf_step
                if progress_callback:
                    encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                    progress_callback(f"{encoder_label} CRF keresés (VMAF fallback: {format_localized_number(min_vmaf, decimals=2)})")
                continue
        except FileNotFoundError as e:
            # WinError 2 or FileNotFoundError - ab-av1.exe not found
            error_msg = f"FATAL ERROR: ab-av1.exe not found or cannot be started!\n\nError: {e}\n\nPath: {ABAV1_PATH}\n\nThe program cannot start ab-av1.exe, so CRF search is not possible.\n\nCheck if ab-av1.exe exists at the specified path, or set the correct path in settings."
            print(f"\n{'='*80}")
            print(f"[WARN][WARN][WARN] FATAL ERROR [WARN][WARN][WARN]")
            print(f"{'='*80}")
            print(error_msg)
            print(f"{'='*80}\n")
            if logger:
                try:
                    logger.write(f"\n{'='*80}\n")
                    logger.write(f"[WARN][WARN][WARN] FATAL ERROR [WARN][WARN][WARN]\n")
                    logger.write(f"{'='*80}\n")
                    logger.write(f"{error_msg}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
                except Exception:
                    pass
            log_writer = get_log_writer()
            if log_writer:
                try:
                    log_writer.write(f"\n{'='*80}\n")
                    log_writer.write(f"[WARN][WARN][WARN] FATAL ERROR [WARN][WARN][WARN]\n")
                    log_writer.write(f"{'='*80}\n")
                    log_writer.write(f"{error_msg}\n")
                    log_writer.write(f"{'='*80}\n\n")
                    log_writer.flush()
                except Exception:
                    pass
            cleanup_ab_av1_temp_dirs(input_path.parent)
            raise FileNotFoundError(error_msg) from e
        except (subprocess.SubprocessError, OSError, ValueError, TypeError, AttributeError) as e:
            # Other errors (not FileNotFoundError) - might be VMAF fallback problem
            error_str = str(e)
            # If WinError 2 is in exception text, it's also ab-av1.exe problem
            if "WinError 2" in error_str or "[WinError 2]" in error_str or "The system cannot find the file specified" in error_str:
                error_msg = f"FATAL ERROR: ab-av1.exe not found or cannot be started!\n\nError: {e}\n\nPath: {ABAV1_PATH}\n\nThe program cannot start ab-av1.exe, so CRF search is not possible.\n\nCheck if ab-av1.exe exists at the specified path, or set the correct path in settings."
                print(f"\n{'='*80}")
                print(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]")
                print(f"{'='*80}")
                print(error_msg)
                print(f"{'='*80}\n")
                if logger:
                    try:
                        logger.write(f"\n{'='*80}\n")
                        logger.write(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]\n")
                        logger.write(f"{'='*80}\n")
                        logger.write(f"{error_msg}\n")
                        logger.write(f"{'='*80}\n\n")
                        logger.flush()
                    except Exception:
                        pass
                log_writer = get_log_writer()
                if log_writer:
                    try:
                        log_writer.write(f"\n{'='*80}\n")
                        log_writer.write(f"[WARN][WARN][WARN] VÉGZETES HIBA [WARN][WARN][WARN]\n")
                        log_writer.write(f"{'='*80}\n")
                        log_writer.write(f"{error_msg}\n")
                        log_writer.write(f"{'='*80}\n\n")
                        log_writer.flush()
                    except Exception:
                        pass
                cleanup_ab_av1_temp_dirs(input_path.parent)
                raise FileNotFoundError(error_msg) from e
            else:
                # Other error - might be VMAF fallback problem
                print(f"[ERROR] Ab-av1 exception: {e}")
                cleanup_ab_av1_temp_dirs(input_path.parent)
                break
    
    if stop_event.is_set():
        cleanup_ab_av1_temp_dirs(input_path.parent)
        raise EncodingStopped()

    cleanup_ab_av1_temp_dirs(input_path.parent)
    default_crf = 28 if encoder == 'av1_nvenc' else 32

    # If NVENC VMAF fallback exhausted, switch to SVT-AV1 queue
    if encoder == 'av1_nvenc':
        print(f"[WARN] NVENC VMAF fallback exhausted -> automatically switching to SVT-AV1 queue")
        return (default_crf, min_vmaf, True)  # True = fallback exhausted

    # If SVT-AV1 VMAF fallback also exhausted, no suitable CRF found -> simple copy
    print(f"[WARN] SVT-AV1 VMAF fallback exhausted -> No suitable CRF value found (VMAF >= 85.0 AND file <= 75%)")
    print(f"   Copying video without re-encoding...")
    raise NoSuitableCRFFound("No suitable CRF value found for the given parameters")

def encode_single_attempt(input_path, output_path, cq_value, subtitle_files, encoder='av1_nvenc', status_callback=None, stop_event=None, vmaf_value=None, resize_enabled=False, resize_height=1080, audio_compression_enabled=False, audio_compression_method='fast', svt_preset=2, logger=None, pre_invalid_subtitles=None, cached_video_metadata=None, original_input_path=None, denoise_enabled=False, denoise_params=None, include_audio=True, size_estimation_callback=None, hard_rotate_degrees=0):
    """Execute a single encoding attempt with specified settings.

    Args:
        input_path: Path to input video.
        output_path: Path to output video.
        cq_value: CRF/CQ value to use.
        subtitle_files: List of subtitle files to include.
        encoder: Encoder name.
        status_callback: Callback for status updates.
        stop_event: Event to stop encoding.
        vmaf_value: Target VMAF (for metadata).
        resize_enabled: Whether to resize video.
        resize_height: Target height if resizing.
        audio_compression_enabled: Whether to compress audio.
        audio_compression_method: Audio compression method.
        svt_preset: SVT-AV1 preset.
        logger: Logger instance.
        pre_invalid_subtitles: Pre-validated invalid subtitles.
        cached_video_metadata: Cached video metadata.
        original_input_path: Original input path (if different from input_path).
        denoise_enabled: Whether denoising was applied (affects encoder params).
        denoise_params: Denoising parameters for metadata.
        include_audio: Whether to include audio streams in the output.
        size_estimation_callback: Callback for real-time size estimation during manual encoding.
                                   Called every 30 seconds with (size_mb, encoded_seconds).
        hard_rotate_degrees: Optional hard-rotation on pixels (0/90/180/270).

    Returns:
        bool: True if encoding successful, False otherwise.
    """
    import builtins as _builtins

    def _routed_print(*args, **kwargs):
        """Route prints to worker logger when available."""
        if logger is not None:
            sep = kwargs.get('sep', ' ')
            end = kwargs.get('end', '\n')
            msg = sep.join(str(arg) for arg in args) + end
            try:
                logger.write(msg)
                if kwargs.get('flush', False):
                    logger.flush()
            except Exception:
                _builtins.print(*args, **kwargs)
        else:
            _builtins.print(*args, **kwargs)

    # Keep function-local print isolated from global stdout fallback mixing.
    print = _routed_print

    # OPTIMIZATION: Use cached metadata if available (avoids slow ffprobe on large lossless master)
    use_cached_metadata = cached_video_metadata is not None
    # MEDIUM FIX #8: Path sanitization for security - use sanitized path
    try:
        input_str = sanitize_path(input_path)
    except (FileNotFoundError, ValueError) as e:
        raise ValueError(f"Invalid input path: {e}") from e
    
    # Output path doesn't need to exist, but must be validated
    try:
        output_resolved = output_path.resolve()
        # Check if parent directory exists
        if not output_resolved.parent.exists():
            raise FileNotFoundError(f"Output directory does not exist: {output_resolved.parent}")
        output_str = os.fspath(output_resolved)
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Invalid output path: {e}") from e
    
    # MEDIUM FIX #8: Use sanitized input_str instead of Path object
    # Get video duration and fps for progress display
    if use_cached_metadata and cached_video_metadata.get('duration_seconds') and cached_video_metadata.get('video_fps'):
        # Use cached metadata (fast path, no ffprobe needed)
        duration_seconds = cached_video_metadata['duration_seconds']
        video_fps = cached_video_metadata['video_fps']
    else:
        # Query from file (slow path)
        duration_seconds, video_fps = get_video_info(Path(input_str))
    # MEDIUM FIX #11: Safe None value handling
    if duration_seconds is None or duration_seconds <= 0:
        duration_seconds = 0
    if video_fps is None or video_fps <= 0:
        video_fps = 25.0  # Fallback fps
    duration_hours = int(duration_seconds // 3600)
    duration_mins = int((duration_seconds % 3600) // 60)
    duration_secs = int(duration_seconds % 60)
    
    # Progress tracking variables - safe calculation
    total_frames = int(duration_seconds * video_fps) if duration_seconds > 0 and video_fps > 0 else 0
    
    # Handle separate original input for audio/subtitles/metadata if provided
    # This is used when input_path is a video-only master file
    original_input_idx = 0
    metadata_source_path = Path(input_str)
    color_metadata_source_path = Path(input_str)
    
    if original_input_path:
        try:
            orig_input_str = sanitize_path(original_input_path)
            orig_in_path = Path(orig_input_str)
            # Only add if distinct from input_path
            if orig_in_path.absolute() != Path(input_str).absolute():
                original_input_idx = 1
                metadata_source_path = orig_in_path
                color_metadata_source_path = orig_in_path
        except Exception as e:
            if logger:
                with console_redirect(logger):
                    print(f"[WARN] Warning: Failed to process original input path: {e}")

    # Query color metadata for preservation (HDR support).
    # When encoding from a denoised master, prefer the original source metadata so
    # primaries/transfer do not disappear just because the intermediate master lost them.
    if color_metadata_source_path.absolute() == Path(input_str).absolute() and use_cached_metadata and cached_video_metadata.get('color_metadata'):
        color_metadata = cached_video_metadata['color_metadata']
    else:
        color_metadata = get_video_color_metadata(color_metadata_source_path)
        if use_cached_metadata and cached_video_metadata.get('color_metadata'):
            from .core_audio_video_ops import merge_color_metadata
            color_metadata = merge_color_metadata(color_metadata, cached_video_metadata['color_metadata'])
    processing_range_name = None
    ffmpeg_color_range = None
    range_reason = None
    if color_metadata:
        from .core_audio_video_ops import get_processing_color_range_with_reason, log_color_range_debug
        processing_range_name, ffmpeg_color_range, range_reason = get_processing_color_range_with_reason(color_metadata)
    
    ffmpeg_cmd = [FFMPEG_PATH, '-i', input_str]
    if original_input_idx == 1:
        ffmpeg_cmd.extend(['-i', os.fspath(metadata_source_path)])

    
    # ================================================================================
    # SUBTITLE VALIDATION - Prevent FFmpeg error
    # ================================================================================
    # Validate subtitles BEFORE ENCODING to avoid FFmpeg crash
    # If a subtitle is corrupt/invalid, FFmpeg crashes during embedding
    # 
    # Strategy:
    # - Valid subtitles: Embed in FFmpeg (during encoding)
    # - Invalid subtitles: Skip embedding, but copy to output folder
    #   (caller function uses _copy_invalid_subtitles() for this)
    
    validated_subtitles = []
    skipped_subtitles = []
    
    # Add pre-invalid subtitles (from task) to skipped_subtitles for summary
    if pre_invalid_subtitles:
        skipped_subtitles.extend(pre_invalid_subtitles)
    
    # Logging to worker console (detailed)
    if subtitle_files:
        if logger:
            with console_redirect(logger):
                print(f"\n{'='*80}")
                print(f"[NOTE] SUBTITLE VALIDATION")
                print(f"{'='*80}")
                print(f"Found subtitles: {len(subtitle_files)}")
        else:
            print(f"\n[NOTE] Subtitle validation: {len(subtitle_files)} files")
    
    for sub_path, lang in subtitle_files:
        # Validation: file size, format, content
        is_valid, reason = is_valid_subtitle_file(sub_path)
        
        if is_valid:
            validated_subtitles.append((sub_path, lang))
            # Log successful validation
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  [OK] {sub_path.name}{lang_display} - Valid")
        else:
            skipped_subtitles.append((sub_path, lang, reason))
            # DETAILED error report to worker console
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  [ERROR] {sub_path.name}{lang_display} - INVALID")
                    print(f"     Error: {reason}")
                    print(f"     -> Skipped from FFmpeg embedding")
                    print(f"     -> Will be copied next to output")
            else:
                print(f"  [ERROR] {sub_path.name} - {reason}")
    
    # Only embed validated subtitles into FFmpeg command
    subtitle_files = validated_subtitles
    
    # SUMMARY to worker console (before FFmpeg encoding)
    if logger:
        with console_redirect(logger):
            print(f"\n{'─'*80}")
            if skipped_subtitles:
                print(f"[WARN] SUBTITLE VALIDATION SUMMARY:")
            else:
                print(f"[OK] SUBTITLE VALIDATION SUMMARY:")
            print(f"   • Valid subtitles (FFmpeg embed): {len(validated_subtitles)}")
            if validated_subtitles:
                print(f"\n   Valid subtitles:")
                for sub_path, lang in validated_subtitles:
                    lang_display = f" [{lang}]" if lang else ""
                    print(f"     [OK] {sub_path.name}{lang_display}")
            print(f"   • Invalid subtitles (skipped): {len(skipped_subtitles)}")
            if skipped_subtitles:
                print(f"\n   Invalid subtitles details:")
                for sub_path, lang, reason in skipped_subtitles:
                    lang_display = f" [{lang}]" if lang else ""
                    print(f"     [ERROR] {sub_path.name}{lang_display}: {reason}")
                print(f"\n   [INFO] Invalid subtitles will be copied (not embedded).")
            print(f"{'─'*80}\n")
    elif skipped_subtitles:
        print(f"\n[NOTE] {len(validated_subtitles)} valid, {len(skipped_subtitles)} invalid subtitles")
    
    # ================================================================================
    
    for subtitle_path, _ in subtitle_files:
        # Subtitle path sanitization
        try:
            subtitle_str = sanitize_path(subtitle_path)
        except (FileNotFoundError, ValueError) as e:
            raise ValueError(f"Invalid subtitle path: {subtitle_path} - {e}") from e
        ffmpeg_cmd.extend(['-i', subtitle_str])
    
    # Check for audio dynamic range compression
    use_audio_compression = False
    audio_51_stream_index = None
    compressed_audio_lang = None  # Language of original 5.1 stream (for compressed stream)
    if audio_compression_enabled:
        # MEDIUM FIX #8: Use sanitized input_str, or original source if available
        # If we have a separate original master, we must analyze THAT for audio
        audio_source_str = os.fspath(metadata_source_path)
        
        if check_audio_compression_needed(metadata_source_path):
            # Find 5.1 stream index for default language
            default_lang, _, _ = get_audio_streams_info(metadata_source_path)
            if default_lang:
                # MEDIUM FIX #8: Use sanitized path
                audio_51_stream_index = get_51_audio_stream_index(metadata_source_path, default_lang)
                # If 5.1 stream found, use compression
                if audio_51_stream_index is not None:
                    use_audio_compression = True
                    # Get language of original 5.1 stream
                    try:
                        cmd = [
                            FFPROBE_PATH, '-v', 'error',
                            '-select_streams', f'a:{audio_51_stream_index}',
                            '-show_entries', 'stream_tags=language',
                            '-of', 'default=noprint_wrappers=1:nokey=1',
                            audio_source_str  # Use correct source
                        ]
                        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
                        lang_raw = result.stdout.strip()
                        if lang_raw:
                            # Normalize language code (3-letter -> 2-letter if needed)
                            compressed_audio_lang = normalize_audio_lang(lang_raw)
                            # If 2-letter, get 3-letter version from LANGUAGE_MAP
                            if len(compressed_audio_lang) == 2 and compressed_audio_lang in LANGUAGE_MAP:
                                compressed_audio_lang = LANGUAGE_MAP[compressed_audio_lang]
                            else:
                                # If already 3-letter, use it
                                compressed_audio_lang = lang_raw if len(lang_raw) == 3 else compressed_audio_lang
                    except (ValueError, TypeError, AttributeError, KeyError):
                        # If failed, use default language
                        if default_lang in LANGUAGE_MAP:
                            compressed_audio_lang = LANGUAGE_MAP[default_lang]
                        else:
                            compressed_audio_lang = default_lang
    
    # Video and audio stream mapping
    ffmpeg_cmd.extend(['-map', '0:v:0'])
    
    # Determine original audio count (for compressed stream index)
    original_audio_count = 0
    try:
        # MEDIUM FIX #8: Use sanitized input_str or original source
        audio_source_str = os.fspath(metadata_source_path)
        # FFprobe command to count original audio streams
        count_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            audio_source_str
        ]
        count_result = subprocess.run(count_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        original_audio_count = len([line for line in count_result.stdout.strip().split('\n') if line.strip()])
    except (subprocess.SubprocessError, ValueError, AttributeError):
        # If failed, assume 1 audio stream
        original_audio_count = 1
    
    # Always copy all audio streams (from original source) - IF INCLUDED
    # ffmpeg_cmd.extend(['-map', f'{original_input_idx}:a?'])  # All audio streams from correct input (optional)
    
    # Add audio dynamic range compression filter (if enabled, add 5.1 stream with compression)
    audio_filter_complex = None
    compressed_audio_index = None  # Index of last audio stream (the compressed one)
    
    if include_audio:
        ffmpeg_cmd.extend(['-map', f'{original_input_idx}:a?'])  # All audio streams from correct input (optional)
        
        if use_audio_compression and audio_51_stream_index is not None:
            # If combobox value is translated text, convert it
            method = audio_compression_method
            if method == t('audio_compression_fast'):
                method = 'fast'
            elif method == t('audio_compression_dialogue'):
                method = 'dialogue'

            audio_filter = build_audio_conversion_filter(method)

            from .core_audio_video_ops import apply_audio_offset_to_filter_chain, get_relative_audio_offset_ms
            audio_delay_ms = get_relative_audio_offset_ms(color_metadata, audio_51_stream_index)
            audio_filter, normalized_audio_delay_ms = apply_audio_offset_to_filter_chain(audio_filter, audio_delay_ms)
            if normalized_audio_delay_ms != 0:
                print(f"[INFO] Compressed audio offset preserved for stream a:{audio_51_stream_index}: {normalized_audio_delay_ms}ms")

            # Use filter complex: add 5.1 stream with compression
            # Use proper input index for audio source
            audio_filter_complex = f'[{original_input_idx}:a:{audio_51_stream_index}]{audio_filter}[acompressed]'
            # Add compressed audio stream to mapping
            ffmpeg_cmd.extend(['-map', '[acompressed]'])
            # Index of last audio stream (the compressed one) = count of original audio streams
            compressed_audio_index = original_audio_count
    
    # Get count of embedded subtitles
    embedded_subtitle_count = 0
    try:
        count_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 's',
            '-show_entries', 'stream=index',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(metadata_source_path)  # Use correct source
        ]
        count_result = subprocess.run(count_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=True, timeout=600, startupinfo=get_startup_info())
        embedded_subtitle_count = len([line for line in count_result.stdout.strip().split('\n') if line.strip()])
    except (subprocess.SubprocessError, ValueError, AttributeError):
        # If failed, assume 0 embedded subtitles
        embedded_subtitle_count = 0
    
    if include_audio:
        ffmpeg_cmd.extend(['-map', f'{original_input_idx}:s?']) # Subtitles from correct input (optional)
        
        # Map external subtitles (indices shift if we have 2 inputs)
        # If original_input_idx is 1, it means we have 2 inputs (0 and 1)
        # So external subtitles start at index 2
        ext_sub_start_idx = original_input_idx + 1
        
        for idx in range(len(subtitle_files)):
            ffmpeg_cmd.extend(['-map', f'{ext_sub_start_idx + idx}:0'])
            
        # Copy chapters and metadata from original source
        ffmpeg_cmd.extend(['-map_chapters', str(original_input_idx)])
        ffmpeg_cmd.extend(['-map_metadata', str(original_input_idx)])
        # And attachments (fonts) too - CRITICAL for subtitle rendering/preservation
        ffmpeg_cmd.extend(['-map', f'{original_input_idx}:t?'])
    
    # Build video filter chain (hard-rotate + optional resize)
    video_filters = []

    # Hard rotate is relative and applied only during re-encode (pixel transform).
    try:
        normalized_hard_rotate = int(float(hard_rotate_degrees)) % 360
    except (ValueError, TypeError):
        normalized_hard_rotate = 0
    if normalized_hard_rotate not in (0, 90, 180, 270):
        normalized_hard_rotate = 0

    if normalized_hard_rotate == 90:
        video_filters.append('transpose=1')
    elif normalized_hard_rotate == 180:
        video_filters.append('transpose=1,transpose=1')
    elif normalized_hard_rotate == 270:
        video_filters.append('transpose=2')

    # Add resize filter if enabled (downscale only, never upscale)
    if resize_enabled:
        # Resize based on shorter side pixel count (max height/width limit)
        # MEDIUM FIX #8: Use sanitized input_str
        # Import the resolution query function
        from .core_audio_video_ops import get_video_resolution
        video_width, video_height = get_video_resolution(Path(input_str))
        if video_width and video_height and video_width > 0 and video_height > 0:
            # Determine shorter side
            shorter_side = min(video_width, video_height)
            # resize_height is now the shorter side pixel count (max limit)
            target_shorter_side = resize_height
            
            # Only apply resize if shorter side EXCEEDS target (downscale only)
            if shorter_side > target_shorter_side:
                # Calculate ratio (avoid ZeroDivisionError)
                if shorter_side > 0:
                    scale_ratio = target_shorter_side / shorter_side
                else:
                    scale_ratio = 1.0
                
                # Calculate new dimensions
                new_width = int(video_width * scale_ratio)
                new_height = int(video_height * scale_ratio)
                
                # Round to even numbers (required for video encoding)
                new_width = new_width if new_width % 2 == 0 else new_width + 1
                new_height = new_height if new_height % 2 == 0 else new_height + 1
                
                video_filters.append(f'scale={new_width}:{new_height}')
            # else: shorter_side <= target_shorter_side -> no resize needed (don't upscale)
        else:
            # If resolution query failed, use old method (based on height) - but only for downscale
            # We can't determine if it needs downscale without resolution, so skip resize
            pass  # Don't apply resize if we can't determine source resolution

    if processing_range_name:
        wrapped_filters = [f'scale=iw:ih:in_range={processing_range_name}:out_range={processing_range_name}']
        wrapped_filters.extend(video_filters)
        wrapped_filters.append(f'scale=iw:ih:in_range={processing_range_name}:out_range={processing_range_name}')
        video_filters = wrapped_filters

    if video_filters:
        ffmpeg_cmd.extend(['-vf', ','.join(video_filters)])

    # If hard rotate is used, force output display metadata rotation to 0.
    # NOTE: `-display_rotation` is an input-side option in ffmpeg 8.x;
    # placing it here (output section) causes "input option/output option" error.
    if normalized_hard_rotate != 0:
        ffmpeg_cmd.extend([
            '-metadata:s:v:0', 'rotate=0',
            '-metadata:s:v:0', 'ROTATE=0',
        ])
    
    # Add audio filter complex if enabled
    if audio_filter_complex:
        ffmpeg_cmd.extend(['-filter_complex', audio_filter_complex])
    
    # Determine target pixel format matching source chroma subsampling (prefer 10-bit for AV1)
    target_pix_fmt = 'yuv420p10le'  # Default
    if color_metadata and color_metadata.get('pix_fmt'):
        src_pix = color_metadata['pix_fmt']
        if '444' in src_pix:
            target_pix_fmt = 'yuv444p10le'
        elif '422' in src_pix:
            target_pix_fmt = 'yuv422p10le'
            
    # Video encoder settings
    # NOTE: metadata_str is set here but applied AFTER -map_metadata to prevent being overwritten
    metadata_str = None
    
    if encoder == 'svt-av1':
        # 8K+ resolution requires preset >= 5 (M5) for SVT-AV1
        from .core_audio_video_ops import get_video_resolution
        _width_8k, _height_8k = get_video_resolution(input_path)
        if _width_8k and _height_8k and (_width_8k >= 7680 or _height_8k >= 4320) and svt_preset < 5:
            print(f"[INFO] 8K videó ({_width_8k}x{_height_8k}), SVT-AV1 preset emelve: {svt_preset} -> 5 (M5 minimum)")
            svt_preset = 5

        # SVT-AV1 encoder parameters depend on whether denoising was applied
        # User decision: Use same high-quality parameters for normal re-encoding as for denoising
        # New logic: Always use the high-quality param set
        svt_params = 'tune=0:aq-mode=2:enable-qm=1:qm-min=0:film-grain=4:enable-dlf=0'

        ffmpeg_cmd.extend(['-c:v', 'libsvtav1', '-preset', str(svt_preset), '-crf', str(int(cq_value)), '-g', '240', '-pix_fmt', target_pix_fmt, '-svtav1-params', svt_params, '-stats_period', '0.5'])
        # Prepare metadata for SVT-AV1 (will be added after -map_metadata)
        # Always write metadata, even if VMAF is unknown (manual encode)
        # CRITICAL: If resize is enabled, VMAF value is invalid (different resolution)
        if resize_enabled:
            vmaf_str = "N/A (Resized)"  # Resize invalidates VMAF comparison
        elif vmaf_value is not None and vmaf_value != "-":
            vmaf_str = f"{vmaf_value:.2f}" if isinstance(vmaf_value, (int, float)) else str(vmaf_value)
        else:
            vmaf_str = "Manual"  # Manual encode without VMAF prediction
        metadata_str = f"FFMPEG SVT-AV1 - CRF:{int(cq_value)} - Preset {svt_preset} - Planned VMAF: {vmaf_str} - {svt_params}"
        if denoise_params:
            metadata_str += f" - {denoise_params}"
    else:
        # NVENC: AV1 NVENC p010le is standard for 10-bit 4:2:0. 
        # Note: NVENC does not support 4:2:2 10-bit for AV1. It will fallback to 4:2:0 via p010le.
        nvenc_pix_fmt = 'p010le'
        if target_pix_fmt == 'yuv444p10le':
            nvenc_pix_fmt = 'yuv444p10le'
            
        ffmpeg_cmd.extend(['-c:v', 'av1_nvenc', '-preset', 'p7', '-tune', 'hq', '-rc', 'vbr', '-cq', str(int(cq_value)), '-multipass', 'fullres', '-pix_fmt', nvenc_pix_fmt, '-stats_period', '0.5'])
        # Prepare metadata for NVENC (will be added after -map_metadata)
        # Always write metadata, even if VMAF is unknown (manual encode)
        # CRITICAL: If resize is enabled, VMAF value is invalid (different resolution)
        if resize_enabled:
            vmaf_str = "N/A (Resized)"  # Resize invalidates VMAF comparison
        elif vmaf_value is not None and vmaf_value != "-":
            vmaf_str = f"{vmaf_value:.2f}" if isinstance(vmaf_value, (int, float)) else str(vmaf_value)
        else:
            vmaf_str = "Manual"  # Manual encode without VMAF prediction
        metadata_str = f"FFMPEG NVENC - CQ:{int(cq_value)} - Preset 7 - Planned VMAF: {vmaf_str}"
        if denoise_params:
            metadata_str += f" - {denoise_params}"
    
    # Add color metadata for preservation (SDR/HDR support)
    if color_metadata:
        if color_metadata.get('color_space'):
            ffmpeg_cmd.extend(['-colorspace', color_metadata['color_space']])
        if color_metadata.get('color_primaries'):
            ffmpeg_cmd.extend(['-color_primaries', color_metadata['color_primaries']])
        if color_metadata.get('color_transfer'):
            ffmpeg_cmd.extend(['-color_trc', color_metadata['color_transfer']])
        if ffmpeg_color_range:
            ffmpeg_cmd.extend(['-color_range', ffmpeg_color_range])
        # HDR metadata (for HDR10 content)
        # CRITICAL: These options are ONLY supported by NVENC encoders (av1_nvenc, hevc_nvenc)!
        # SVT-AV1 does NOT support -master_display and -max_cll at the FFmpeg level.
        # For SVT-AV1, HDR metadata should be passed via side data, not command line flags.
        if encoder == 'av1_nvenc':
            if color_metadata.get('master_display'):
                ffmpeg_cmd.extend(['-master_display', color_metadata['master_display']])
            if color_metadata.get('max_content') and color_metadata.get('max_average'):
                ffmpeg_cmd.extend(['-max_cll', f"{color_metadata['max_content']},{color_metadata['max_average']}"])

        # Preserve effective display aspect ratio explicitly.
        # This covers anamorphic sources, source display rotation, and hard-rotate.
        output_width = None
        output_height = None
        try:
            output_width, output_height = get_video_resolution(Path(input_str))
        except Exception:
            output_width, output_height = None, None

        effective_rotation = normalized_hard_rotate
        try:
            effective_rotation = get_effective_output_rotation_degrees(Path(input_str), normalized_hard_rotate)
        except Exception:
            pass

        adjusted_dar = get_adjusted_display_aspect_ratio(
            color_metadata,
            width=output_width,
            height=output_height,
            rotation_degrees=effective_rotation
        )
        if adjusted_dar:
            ffmpeg_cmd.extend(['-aspect', adjusted_dar])

    av1_metadata_bsf_args = []
    if encoder in ('svt-av1', 'av1_nvenc') and color_metadata:
        from .core_audio_video_ops import build_av1_metadata_bsf_args
        av1_metadata_bsf_args = build_av1_metadata_bsf_args(color_metadata)
        if av1_metadata_bsf_args:
            ffmpeg_cmd.extend(av1_metadata_bsf_args)

    # Audio codec settings
    if include_audio:
        if use_audio_compression and audio_51_stream_index is not None and compressed_audio_index is not None:
            # First copy all audio streams
            ffmpeg_cmd.extend(['-c:a', 'copy'])
            # Encode last audio stream (compressed) to AAC - use stream specifier
            ffmpeg_cmd.extend([f'-c:a:{compressed_audio_index}', 'aac', f'-b:a:{compressed_audio_index}', '192k', f'-ac:{compressed_audio_index}', '2'])
            # Add metadata to compressed audio stream: language and 2.0 label
            if compressed_audio_lang:
                ffmpeg_cmd.extend([f'-metadata:s:a:{compressed_audio_index}', f'language={compressed_audio_lang}'])
            title_text = get_audio_conversion_title(method)
            ffmpeg_cmd.extend([f'-metadata:s:a:{compressed_audio_index}', f'title={title_text}'])
        else:
            ffmpeg_cmd.extend(['-c:a', 'copy'])
        
        # Copy embedded subtitles - first general setting
        ffmpeg_cmd.extend(['-c:s', 'copy'])
        # Convert external subtitles to SRT - use positive stream index
        # External subtitles stream index: embedded_subtitle_count + external_subtitle_index
        for idx in range(len(subtitle_files)):
            external_subtitle_stream_idx = embedded_subtitle_count + idx
            ffmpeg_cmd.extend([f'-c:s:{external_subtitle_stream_idx}', 'srt'])
        
        for idx, (subtitle_path, lang_part) in enumerate(subtitle_files):
            iso_lang = normalize_language_code(lang_part)
            # External subtitles stream index follows embedded ones
            external_subtitle_stream_idx = embedded_subtitle_count + idx
            ffmpeg_cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'language={iso_lang}'])
            if lang_part:
                title = lang_part if '-' in lang_part else lang_part.upper()
                ffmpeg_cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'title={title}'])
        
        
    else:
        # Video-only mode: Explicitly disable audio, subtitles, and data streams implies strictly video
        ffmpeg_cmd.extend(['-an', '-sn', '-dn'])

    # CRITICAL: Add encoding metadata AFTER -map_metadata to prevent being overwritten
    # The -map_metadata copies all metadata from source, then we add our Settings tag on top
    # We write to BOTH global metadata and video stream metadata for maximum compatibility
    if metadata_str:
        ffmpeg_cmd.extend(['-metadata', f'Settings={metadata_str}'])
        ffmpeg_cmd.extend(['-metadata:s:v:0', f'Settings={metadata_str}'])

    # EXTRA METADATA FIX: Extract and copy ALL global tags from source
    # User requested NO filtering (even stats tags), except ensuring new Settings are not overwritten.
    try:
        from .core_audio_video_ops import extract_all_global_tags
        # Prefer original_input_path (original source) over input_path (which might be denoise master)
        meta_source = original_input_path if original_input_path and original_input_path.exists() else input_path
        
        source_global_tags = extract_all_global_tags(meta_source)
        for key, value in source_global_tags.items():
            if key == 'Settings':
                continue
            ffmpeg_cmd.extend(['-metadata', f'{key}={value}'])
    except Exception:
        pass

    ffmpeg_cmd.extend(['-y', output_str])
    
    # Write FFmpeg command to console
    # If logger exists, it goes through console_redirect() context manager
    # If no logger, it goes to sys.stdout (rare case)
    ffmpeg_cmd_str = format_cmd_for_windows(ffmpeg_cmd)
    print(f"\n{'='*80}")
    print(f"🎬 FFMPEG PARANCS (CQ/CRF: {int(cq_value)}):")
    print(f"{'='*80}")
    if processing_range_name:
        log_color_range_debug(print, processing_range_name, range_reason, ffmpeg_color_range)
    if color_metadata:
        print(f"Color Metadata Source: {color_metadata_source_path}")
    if av1_metadata_bsf_args:
        print(f"AV1 Bitstream Metadata Fix: {' '.join(av1_metadata_bsf_args)}")
    print(ffmpeg_cmd_str)
    print(f"{'='*80}\n")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    if stop_event.is_set():
        raise EncodingStopped()

    try:
        # IMPORTANT: We do NOT set cwd because there might be identical filenames in different folders
        # Using absolute paths ensures we use the correct files
        with subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, encoding='utf-8', errors='replace', bufsize=1, shell=False, startupinfo=get_startup_info()) as process:
            
            # Process registration
            with ACTIVE_PROCESSES_LOCK:
                ACTIVE_PROCESSES.append(process)

            # Initialize size estimation tracking
            last_size_update = 0.0 if size_estimation_callback else None

            try:
                for line in process.stdout:
                    if stop_event.is_set():
                        process.kill()
                        process.wait()
                        raise EncodingStopped()
                    # Write FFmpeg output to worker logger when available.
                    try:
                        if logger is not None:
                            logger.write(line)
                        else:
                            import sys
                            if hasattr(sys.stdout, 'write'):
                                sys.stdout.write(line)
                                sys.stdout.flush()
                    except (OSError, IOError, AttributeError):
                        pass
                    if status_callback:
                        # Only process frame= lines for progress calculation (ignore warning messages)
                        if line.strip().startswith('frame='):
                            # Extract frame number
                            frame_match = re.search(r'frame=\s*(\d+)', line)
                            if frame_match and total_frames > 0:
                                current_frame = int(frame_match.group(1))
                                # Progress calculation based on frame: current_frame / total_frames * duration
                                if video_fps > 0:
                                    current_time = current_frame / video_fps
                                else:
                                    # Fallback: if no fps, calculate proportionally
                                    current_time = (current_frame / total_frames) * duration_seconds if total_frames > 0 else 0

                                # Limit to video duration
                                current_time = min(current_time, duration_seconds) if duration_seconds > 0 else current_time

                                progress_hours = int(current_time // 3600)
                                progress_mins = int((current_time % 3600) // 60)
                                progress_secs = int(current_time % 60)
                                status_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
                            elif duration_seconds > 0:
                                # Fallback: if no frame number, use elapsed
                                elapsed_total = 0
                                if 'elapsed=' in line:
                                    elapsed_match = re.search(r'elapsed=(\d+):(\d+):(\d+)(?:\.\d+)?', line)
                                    if elapsed_match:
                                        elapsed_hours, elapsed_mins, elapsed_secs = map(int, elapsed_match.groups()[:3])
                                        elapsed_total = elapsed_hours * 3600 + elapsed_mins * 60 + elapsed_secs

                                if elapsed_total > 0:
                                    # Dynamic estimation: based on elapsed, but limited to video duration
                                    estimated_progress = min(elapsed_total * 2, duration_seconds)
                                    progress_hours = int(estimated_progress // 3600)
                                    progress_mins = int((estimated_progress % 3600) // 60)
                                    progress_secs = int(estimated_progress % 60)
                                    status_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")

                    # Size estimation callback (every 30 seconds)
                    if size_estimation_callback and line.strip().startswith('frame='):
                        import time as time_module
                        current_time_val = time_module.time()
                        if last_size_update is not None and (current_time_val - last_size_update >= 30):
                            # Parse size=XXXKiB or size=XXXMiB
                            size_match = re.search(r'size=\s*(\d+(?:\.\d+)?)(KiB|MiB|GiB)', line)
                            time_match = re.search(r'time=\s*(\d{2}):(\d{2}):(\d{2})(?:\.(\d{2}))?', line)

                            if size_match and time_match:
                                try:
                                    size_val = float(size_match.group(1))
                                    size_unit = size_match.group(2)
                                    # Convert to MB
                                    if size_unit == 'KiB':
                                        size_mb = size_val / 1024
                                    elif size_unit == 'MiB':
                                        size_mb = size_val
                                    elif size_unit == 'GiB':
                                        size_mb = size_val * 1024
                                    else:
                                        size_mb = 0

                                    h = int(time_match.group(1))
                                    m = int(time_match.group(2))
                                    s = int(time_match.group(3))
                                    ms = int(time_match.group(4)) if time_match.group(4) else 0
                                    encoded_seconds = h * 3600 + m * 60 + s + ms / 100.0

                                    size_estimation_callback(size_mb, encoded_seconds)
                                    last_size_update = current_time_val
                                except (ValueError, TypeError):
                                    pass
            except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
                print(f"FFmpeg output reading error: {e}")
            
            try:
                process.wait()
            except (OSError, subprocess.SubprocessError) as e:
                print(f"FFmpeg wait error: {e}")
                if process.poll() is None:
                    process.kill()
                    process.wait()
            finally:
                # Remove process from list
                with ACTIVE_PROCESSES_LOCK:
                    if process in ACTIVE_PROCESSES:
                        ACTIVE_PROCESSES.remove(process)
        
        if stop_event.is_set():
            raise EncodingStopped()

        success = process.returncode == 0
        
        debug_pause(
            f"FFmpeg finished: {'OK' if success else 'ERROR'} (CQ: {int(cq_value)})",
            "Validation" if success else "Retry",
            f"Output: {output_path}"
        )
        
        # Return success and skipped subtitles (invalid subtitles found during validation)
        return success, skipped_subtitles
    except EncodingStopped:
        raise
    except (subprocess.SubprocessError, OSError, ValueError, TypeError, AttributeError) as e:
        print(f"[ERROR] Encoding error: {e}")
        return False, []

def encode_video(input_path, output_path, initial_cq_value, subtitle_files, encoder='av1_nvenc', status_callback=None, initial_min_vmaf=None, vmaf_step=None, max_encoded_percent=None, stop_event=None, vmaf_value=None, resize_enabled=False, resize_height=1080, audio_compression_enabled=False, audio_compression_method='fast', svt_preset=2, logger=None, pre_invalid_subtitles=None, cached_video_metadata=None, original_input_path=None, denoise_enabled=False, denoise_params=None, hard_rotate_degrees=0):
    """Main video encoding workflow.
    
    Handles the entire encoding process including:
    - Audio stream analysis
    - CRF search (if needed)
    - Encoding execution
    - Validation
    - Retry logic with adjusted settings (VMAF fallback)
    
    Args:
        input_path: Path to input video.
        output_path: Path to output video.
        initial_cq_value: Initial CRF/CQ value.
        subtitle_files: List of subtitle files.
        encoder: Encoder name.
        status_callback: Callback for status updates.
        initial_min_vmaf: Target VMAF.
        vmaf_step: VMAF step size.
        max_encoded_percent: Max size percentage.
        stop_event: Event to stop process.
        vmaf_value: Current VMAF value.
        resize_enabled: Resize option.
        resize_height: Target height.
        audio_compression_enabled: Audio compression option.
        audio_compression_method: Audio compression method.
        svt_preset: SVT preset.
        logger: Logger instance.
        hard_rotate_degrees: Optional hard-rotation on pixels (0/90/180/270).
        
    Returns:
        tuple: (bool, list) - (True if successful, list of additional skipped subtitles) or (False, []) on error.
    """
    if not input_path.exists():
        print(f"[ERROR] Source file does not exist: {input_path}")
        return False, []
    
    # CRITICAL PROTECTION: Save file identifiers BEFORE encoding
    # This protects against mixing CRF values between different videos
    input_absolute_encode_start = input_path.absolute()
    try:
        input_stat_encode_start = input_path.stat()
        input_size_encode_start = input_stat_encode_start.st_size
        input_mtime_encode_start = input_stat_encode_start.st_mtime
    except (OSError, PermissionError) as e:
        print(f"[ERROR] Encoding: failed to stat file: {input_path} - {e}")
        return False, []
    
    original_size = input_size_encode_start
    cq_value = initial_cq_value
    max_cq = 51 if encoder == 'av1_nvenc' else 63

    initial_min_vmaf, vmaf_step, max_encoded_percent = resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent)
    current_vmaf = initial_min_vmaf if vmaf_value is None else vmaf_value

    if stop_event is None:
        stop_event = STOP_EVENT
    
    # Print audio stream information before CQ/CRF search
    print(f"\n{'='*80}")
    print(f"🔊 AUDIO STREAM ANALYSIS: {input_path.name}")
    print(f"{'='*80}")
    
    audio_analysis_source = original_input_path if original_input_path else input_path
    
    try:
        default_lang, lang_51_count, lang_20_count = get_audio_streams_info(audio_analysis_source)
        print(f"Default language: {default_lang if default_lang else 'None'}")
        
        if lang_51_count or lang_20_count:
            print(f"\nAudio streams by language:")
            all_langs = set(list(lang_51_count.keys()) + list(lang_20_count.keys()))
            for lang in sorted(all_langs):
                count_51 = lang_51_count.get(lang, 0)
                count_20 = lang_20_count.get(lang, 0)
                lang_display = lang if lang != 'unknown' else 'Unknown'
                if count_51 > 0:
                    print(f"  - {lang_display}: {count_51} 5.1 stream(s)")
                if count_20 > 0:
                    print(f"  - {lang_display}: {count_20} 2.0 stream(s)")
        else:
            print(f"No audio stream information")
        
        # Check audio dynamic range compression
        if audio_compression_enabled:
            print(f"\n🔊 Audio dynamic range compression: ENABLED (method: {audio_compression_method})")
            if check_audio_compression_needed(audio_analysis_source):
                if default_lang:
                    audio_51_stream_index = get_51_audio_stream_index(audio_analysis_source, default_lang)
                    if audio_51_stream_index is not None:
                        print(f"  [OK] 5.1 stream found for default language (index: {audio_51_stream_index})")
                        print(f"  [OK] New 2.0 stream will be added with dynamic compression")
                    else:
                        print(f"  [ERROR] 5.1 stream not found for default language")
                else:
                    print(f"  [ERROR] Default language not found")
            else:
                print(f"  [ERROR] Compression not needed (2.0 stream exists or no 5.1)")
        else:
            print(f"\n🔊 Audio dynamic range compression: DISABLED")

        # Calculate and display track sizes
        try:
            print(f"\n[STATS] TRACK SIZE ANALYSIS:")
            total_file_size = audio_analysis_source.stat().st_size
            total_file_size_mb = total_file_size / (1024 * 1024)

            # Get exact video stream size
            video_size_bytes = get_video_stream_size_bytes_exact(audio_analysis_source, timeout=600)

            if video_size_bytes and video_size_bytes > 0:
                video_size_mb = video_size_bytes / (1024 * 1024)
                # Audio + subtitles + attachments + overhead = Total - Video
                other_size_bytes = max(0, total_file_size - video_size_bytes)
                other_size_mb = other_size_bytes / (1024 * 1024)

                print(f"  [OK] Video stream size: {video_size_mb:.1f} MB")
                print(f"  [OK] Audio + Other tracks (subtitles, attachments, overhead): {other_size_mb:.1f} MB")
                print(f"  [OK] Total file size: {total_file_size_mb:.1f} MB")
            else:
                print(f"  [WARN] Video stream size measurement failed")
                print(f"  [OK] Total file size: {total_file_size_mb:.1f} MB")
        except Exception as size_error:
            print(f"  [WARN] Track size analysis error: {size_error}")

        print(f"{'='*80}\n")
    except Exception as e:
        print(f"[WARN] Audio stream analysis error: {e}\n")
    
    additional_skipped_subtitles = []

    while cq_value <= max_cq:
        if stop_event.is_set():
            raise EncodingStopped()

        success, additional_skipped_subtitles = encode_single_attempt(input_path, output_path, cq_value, subtitle_files, encoder, status_callback, stop_event=stop_event, vmaf_value=current_vmaf, resize_enabled=resize_enabled, resize_height=resize_height, audio_compression_enabled=audio_compression_enabled, audio_compression_method=audio_compression_method, svt_preset=svt_preset, logger=logger, pre_invalid_subtitles=pre_invalid_subtitles, cached_video_metadata=cached_video_metadata, original_input_path=original_input_path, denoise_enabled=denoise_enabled, denoise_params=denoise_params, hard_rotate_degrees=hard_rotate_degrees)
        
        # Note: additional_skipped_subtitles are handled by the caller (worker functions)
        # They will be copied separately
        
        if not success:
            if output_path.exists() and not DEBUG_MODE:
                output_path.unlink()
            elif output_path.exists() and DEBUG_MODE:
                 print(f"  [STOP] DEBUG: Output KEPT (failed/stopped): {output_path}")
            return False, additional_skipped_subtitles
        
        if not output_path.exists():
            return False, additional_skipped_subtitles
        
        new_size = output_path.stat().st_size
        
        if new_size < original_size:
            return True, additional_skipped_subtitles
        else:
            if current_vmaf > 85.0:
                current_vmaf_str = format_localized_number(current_vmaf, decimals=2)
                next_vmaf_str = format_localized_number(current_vmaf - vmaf_step, decimals=2)
                print(f"\n[WARN] File larger, VMAF reduction: {current_vmaf_str} -> {next_vmaf_str}")
                
                new_mb = new_size / (1024**2)
                orig_mb = original_size / (1024**2)
                new_mb_str = format_localized_number(new_mb, decimals=1)
                orig_mb_str = format_localized_number(orig_mb, decimals=1)
                debug_pause(
                    f"File larger ({new_mb_str} > {orig_mb_str} MB)",
                    f"VMAF reduction -> new CRF search",
                    f"File: {output_path}"
                )
                
                current_vmaf -= vmaf_step
                cq_result = run_crf_search(input_path, encoder, current_vmaf, vmaf_step, max_encoded_percent, stop_event=stop_event, svt_preset=svt_preset)
                
                # CRITICAL PROTECTION: Check AFTER CRF search
                # Ensure it is the SAME file as when encoding started
                if not input_path.exists():
                    raise FileNotFoundError(f"FATAL ERROR: Source file DISAPPEARED during encoding!\n"
                                           f"File: {input_absolute_encode_start}\n"
                                           f"This means the CRF value is invalid!")
                
                # Check: SAME absolute path*
                input_absolute_encode_check = input_path.absolute()
                if input_absolute_encode_check != input_absolute_encode_start:
                    raise ValueError(f"FATAL ERROR: Source file CHANGED during encoding!\n"
                                   f"Start of encoding: {input_absolute_encode_start}\n"
                                   f"After CRF search: {input_absolute_encode_check}\n"
                                   f"This means the CRF value belongs to ANOTHER VIDEO!\n"
                                   f"The program stops immediately for safety.")
                
                # Check: SAME file size and modification date*
                try:
                    input_stat_encode_check = input_path.stat()
                    input_size_encode_check = input_stat_encode_check.st_size
                    input_mtime_encode_check = input_stat_encode_check.st_mtime
                    
                    if input_size_encode_check != input_size_encode_start:
                        raise ValueError(f"FATAL ERROR: Source file SIZE CHANGED during encoding!\n"
                                       f"Size at start: {input_size_encode_start:,} bytes\n"
                                       f"Size after CRF search: {input_size_encode_check:,} bytes\n"
                                       f"This means the file was modified, and the CRF value is invalid!")
                    
                    if abs(input_mtime_encode_check - input_mtime_encode_start) > 1.0:
                        raise ValueError(f"FATAL ERROR: Source file MODIFICATION DATE CHANGED during encoding!\n"
                                       f"Date at start: {datetime.fromtimestamp(input_mtime_encode_start).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                       f"Date after CRF search: {datetime.fromtimestamp(input_mtime_encode_check).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                       f"This means the file was modified, and the CRF value is invalid!")
                except (OSError, PermissionError) as e:
                    raise FileNotFoundError(f"FATAL ERROR: Failed to verify file after CRF search: {e}")
                
                if isinstance(cq_result, tuple) and len(cq_result) == 3 and cq_result[2] is True and encoder == 'av1_nvenc':
                    raise NVENCFallbackRequired("NVENC VMAF fallback exhausted during encode_video()")
                new_cq = cq_result[0] if isinstance(cq_result, tuple) else cq_result
                
                if output_path.exists() and not DEBUG_MODE:
                    output_path.unlink()
                elif output_path.exists() and DEBUG_MODE:
                    print(f"  [STOP] DEBUG: Temp output KEPT (file too large): {output_path}")
                
                cq_value = new_cq
                continue
            
            if cq_value >= max_cq:
                if output_path.exists() and not DEBUG_MODE:
                    output_path.unlink()
                return False, additional_skipped_subtitles
            
            cq_value += 1
            
            if output_path.exists() and not DEBUG_MODE:
                output_path.unlink()
            elif output_path.exists() and DEBUG_MODE:
                 print(f"  [STOP] DEBUG: Output KEPT (failed/stopped): {output_path}")

    return False, additional_skipped_subtitles

def remove_audio_track_from_file(source_path, audio_index, logger=None, stop_event=None):
    """Remove a specific audio track from the video file.
    
    Args:
        source_path: Path to the video file.
        audio_index: Index of the audio track to remove (0-based).
        logger: Logger instance.
        stop_event: Event to stop process.
        
    Returns:
        bool: True if successful, False otherwise.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    source_path = Path(source_path)
    source_suffix = source_path.suffix or '.mkv'
    safe_stem = source_path.stem or source_path.name
    temp_output = source_path.with_name(f"{safe_stem}.audioedit{source_suffix}")
    backup_path = source_path.with_name(source_path.name + ".audioedit.bak")

    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(source_path),
        '-map', '0',
        '-c', 'copy',
        '-map', f'-0:a:{audio_index}',
        '-map_chapters', '0',
        '-map_metadata', '0',
    ]
    

        
    cmd.append(os.fspath(temp_output))

    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"AUDIO TRACK REMOVAL: {source_path.name}\n")
        logger.write(f"COMMAND: {format_cmd_for_windows(cmd)}\n")
        logger.write(f"{'='*80}\n")
        logger.flush()

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
        universal_newlines=True,
        startupinfo=get_startup_info()
    ) as process:
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)

        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print(f"[WARN] FFmpeg audio track removal graceful termination timeout (>5s) - Killing process")
                        process.kill()
                    raise EncodingStopped()
                if logger:
                    logger.write(line)
            process.wait()
        finally:
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)

    if process.returncode != 0:
        if temp_output.exists() and not DEBUG_MODE:
            temp_output.unlink()
        elif temp_output.exists() and DEBUG_MODE:
             print(f"  [STOP] DEBUG: Audio removal temp KEPT: {temp_output}")
        raise RuntimeError(f"FFmpeg audio track removal error (rc={process.returncode})")

    original_replaced = False
    restore_succeeded = False
    try:
        if backup_path.exists():
            backup_path.unlink()
        source_path.replace(backup_path)
        original_replaced = True
        temp_output.replace(source_path)
    except (OSError, PermissionError, FileNotFoundError, shutil.Error) as e:
        # File operation error - try to restore original file
        if original_replaced and backup_path.exists():
            try:
                backup_path.replace(source_path)
                restore_succeeded = True
            except (OSError, PermissionError, FileNotFoundError, shutil.Error):
                # If restore also fails, log but don't block
                pass
        if temp_output.exists():
            try:
                temp_output.unlink()
            except (OSError, PermissionError):
                pass
        raise
    finally:
        if not DEBUG_MODE:
            if backup_path.exists() and (not original_replaced or restore_succeeded):
                try:
                    backup_path.unlink()
                except OSError:
                    pass
            if temp_output.exists():
                try:
                    temp_output.unlink()
                except OSError:
                    pass
        else:
            if backup_path.exists():
                print(f"  [STOP] DEBUG: Audio removal backup KEPT: {backup_path}")
            if temp_output.exists():
                print(f"  [STOP] DEBUG: Audio removal temp KEPT: {temp_output}")

    return True

def convert_audio_track_to_stereo(source_path, audio_index, method='fast', language_code=None, logger=None, stop_event=None):
    """Convert a specific audio track to stereo (2.0).
    
    Args:
        source_path: Path to the video file.
        audio_index: Index of the audio track to convert.
        method: Conversion method ('fast' or 'high_quality').
        language_code: Language code for metadata.
        logger: Logger instance.
        stop_event: Event to stop process.
        
    Returns:
        bool: True if successful, False otherwise.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    source_path = Path(source_path)
    source_suffix = source_path.suffix or '.mkv'
    safe_stem = source_path.stem or source_path.name
    temp_output = source_path.with_name(f"{safe_stem}.audioconv{source_suffix}")
    backup_path = source_path.with_name(source_path.name + ".audioconv.bak")

    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    audio_details = get_audio_stream_details(source_path)
    new_audio_index = len(audio_details)
    filter_chain = build_audio_conversion_filter(method)
    from .core_audio_video_ops import apply_audio_offset_to_filter_chain, get_relative_audio_offset_ms
    source_metadata = get_video_color_metadata(source_path)
    audio_delay_ms = get_relative_audio_offset_ms(source_metadata, audio_index)
    filter_chain, normalized_audio_delay_ms = apply_audio_offset_to_filter_chain(filter_chain, audio_delay_ms)
    title_text = get_audio_conversion_title(method)
    filter_label = f"stereo_{audio_index}"
    language_tag = (language_code or '').lower()

    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(source_path),
        '-map', '0',
        '-c', 'copy',
        '-map_chapters', '0',
        '-map_metadata', '0',
        '-filter_complex', f"[0:a:{audio_index}]{filter_chain}[{filter_label}]",
        '-map', f'[{filter_label}]',
        f'-c:a:{new_audio_index}', 'aac',
        f'-b:a:{new_audio_index}', '192k',
        f'-ac:{new_audio_index}', '2',
        f'-metadata:s:a:{new_audio_index}', f'title={title_text}',
    ]


        
    if language_tag:
        cmd.extend([f'-metadata:s:a:{new_audio_index}', f'language={language_tag}'])

    cmd.append(os.fspath(temp_output))

    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"AUDIO 2.0 CONVERSION: {source_path.name} (index: {audio_index}, method: {method})\n")
        if normalized_audio_delay_ms != 0:
            logger.write(f"OFFSET PRESERVATION: {normalized_audio_delay_ms} ms\n")
        logger.write(f"COMMAND: {format_cmd_for_windows(cmd)}\n")
        logger.write(f"{'='*80}\n")
        logger.flush()

    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
        universal_newlines=True,
        startupinfo=get_startup_info()
    ) as process:
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)

        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print(f"[WARN] FFmpeg audio conversion graceful termination timeout (>5s) - Killing process")
                        process.kill()
                    raise EncodingStopped()
                if logger:
                    logger.write(line)
            process.wait()
        finally:
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)

    if process.returncode != 0:
        if temp_output.exists() and not DEBUG_MODE:
            temp_output.unlink()
        elif temp_output.exists() and DEBUG_MODE:
             print(f"  [STOP] DEBUG: Audio conversion temp KEPT: {temp_output}")
        raise RuntimeError(f"FFmpeg conversion error (rc={process.returncode})")

    original_replaced = False
    restore_succeeded = False
    try:
        if backup_path.exists():
            backup_path.unlink()
        source_path.replace(backup_path)
        original_replaced = True
        temp_output.replace(source_path)
    except (OSError, PermissionError, FileNotFoundError, shutil.Error) as e:
        # File operation error - try to restore original file
        if original_replaced and backup_path.exists():
            try:
                backup_path.replace(source_path)
                restore_succeeded = True
            except (OSError, PermissionError, FileNotFoundError, shutil.Error):
                # If restore also fails, log but don't block
                pass
        if temp_output.exists():
            try:
                temp_output.unlink()
            except (OSError, PermissionError):
                pass
        raise
    finally:
        if not DEBUG_MODE:
            if backup_path.exists() and (not original_replaced or restore_succeeded):
                try:
                    backup_path.unlink()
                except OSError:
                    pass
            if temp_output.exists():
                try:
                    temp_output.unlink()
                except OSError:
                    pass
        else:
            if backup_path.exists():
                print(f"  [STOP] DEBUG: Audio conversion backup KEPT: {backup_path}")
            if temp_output.exists():
                print(f"  [STOP] DEBUG: Audio conversion temp KEPT: {temp_output}")

    return True

def open_video_file(file_path):
    """Open a video file with the default system player.
    
    Args:
        file_path: Path to the video file.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        if not file_path.exists():
            return False
        system = platform.system()
        if system == 'Windows':
            os.startfile(os.fspath(file_path))
        elif system == 'Darwin':
            subprocess.call(['open', os.fspath(file_path)])
        else:
            subprocess.call(['xdg-open', os.fspath(file_path)])
        return True
    except (OSError, subprocess.SubprocessError, FileNotFoundError):
        return False
