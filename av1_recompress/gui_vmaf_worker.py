from .gui_imports import *
from .gui_shared import *

class VmafWorkerMixin:
    _vmaf_stop_lock = threading.Lock()

    def vmaf_worker(self, worker_index=0):
        """Background worker for VMAF/PSNR calculation tasks.
    
        Processes requests for VMAF/PSNR calculation on encoded videos.
        Args:
            worker_index: Index of the worker thread (0-based).
        """
    
        set_low_priority()
        
        debug_pause.gui_queue = self.encoding_queue
        
        # Select logger based on worker_index
        if hasattr(self, 'svt_loggers') and self.svt_loggers:
            logger_idx = worker_index % len(self.svt_loggers)
            svt_logger = self.svt_loggers[logger_idx]
            svt_logger.set_worker_index(worker_index)
        else:
            # Fallback to single logger
            svt_logger = self.svt_logger

        def schedule_vmaf_idle():
            # DEADLOCK FIX: Check if app is closing before calling root.after()
            if hasattr(self, 'root'):
                try:
                    if not is_app_closing() and self.root.winfo_exists():
                        self.root.after(0, self._on_vmaf_worker_finished)
                except tk.TclError:
                    pass  # root already destroyed

        
        while True:
            # Azonnali leállítás ellenőrzése
            if STOP_EVENT.is_set():
                with self._vmaf_stop_lock:
                    if not self._vmaf_stop_drained:
                        self._vmaf_stop_drained = True
                        with console_redirect(svt_logger):
                            print(f"\n{t('log_immediate_stop_vmaf')}\n")
                        # FIX #4: Drain-loop keeps running until queue stays empty.
                        # Prevents losing items that other threads put() between the
                        # drain and re-queue — each pass picks up any newly-arrived tasks.
                        pending_tasks = []
                        while True:
                            found_any = False
                            while not VMAF_QUEUE.empty():
                                try:
                                    task = VMAF_QUEUE.get_nowait()
                                except queue.Empty:
                                    break
                                found_any = True
                                pending_tasks.append(task)
                                item_id = task.get('item_id')
                                if item_id:
                                    waiting_status = self._get_vmaf_waiting_status_text(bool(task.get('check_vmaf', True)), bool(task.get('check_psnr', True)))
                                    self.encoding_queue.put((
                                        "update",
                                        item_id,
                                        waiting_status,
                                        task.get('current_cq_str', "-"),
                                        task.get('current_vmaf_str', "-"),
                                        task.get('current_psnr_str', "-"),
                                        "-",
                                        task.get('orig_size_str', "-"),
                                        task.get('current_new_size_str', "-"),
                                        task.get('current_size_change', "-"),
                                        task.get('current_completed_date', "")
                                    ))
                                    self.encoding_queue.put(("tag", item_id, "pending"))
                                VMAF_QUEUE.task_done()
                            if not found_any:
                                break
                        for queued_task in pending_tasks:
                            VMAF_QUEUE.put(queued_task)
                        schedule_vmaf_idle()
                break
            
            # Sima leállítás ellenőrzése
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            is_enc, _, graceful_stop = self.get_encoding_state()
            if graceful_stop:
                if VMAF_QUEUE.empty():
                    # Ha nincs több feladat, befejezhetjük
                    with console_redirect(svt_logger):
                        print(f"\n{t('log_graceful_stop_vmaf')}\n")
                    schedule_vmaf_idle()
                    break
                # Ha van még feladat, folytatjuk az aktuális feladat feldolgozását, de nem veszünk fel újat

            try:
                task = VMAF_QUEUE.get(timeout=2)
            except queue.Empty:
                # Csak akkor lépünk ki, ha:
                # 1. STOP_EVENT be van állítva, VAGY
                # 2. graceful_stop_requested ÉS a queue üres, VAGY
                # 3. is_encoding hamis ÉS a queue üres (nincs több feladat)
                # Ha van feladat a queue-ban, akkor folytatjuk, még akkor is, ha is_encoding hamis
                # THREAD-SAFETY FIX: Use cached is_enc and graceful_stop from above
                if STOP_EVENT.is_set() or (graceful_stop and VMAF_QUEUE.empty()) or (not is_enc and VMAF_QUEUE.empty()):
                    schedule_vmaf_idle()
                    break
                continue
            
            try:
                video_path = task['video_path']
                if hasattr(self, 'vmaf_processing_videos'):
                    self.vmaf_processing_videos.add(video_path)
                output_file = task['output_file']
                
                # CRITICAL: Extract VMAF reference (original source for denoised videos)
                # Use 'or' to handle None values - dict.get() returns None if key exists with None value!
                vmaf_reference_path = task.get('vmaf_reference_path') or video_path
                denoised_master_to_cleanup = task.get('denoised_master_path', None)
                
                # Log if using different reference
                if vmaf_reference_path != video_path:
                    with console_redirect(svt_logger):
                        print(f"[INFO] Zajszűrött videó: VMAF referencia az eredeti forrás")
                        print(f"   Eredeti: {vmaf_reference_path.name}")
                        print(f"   Denoised master: {video_path.name}")
                
                item_id = task['item_id']
                orig_size_str = task.get('orig_size_str', "-")
                # Tree értékek a task-ból (GUI thread-ben lett lekérve)
                original_cq_str = task.get('current_cq_str', "-")
                original_vmaf_str = task.get('current_vmaf_str', "-")
                original_psnr_str = task.get('current_psnr_str', "-")
                original_new_size_str = task.get('current_new_size_str', "-")
                original_size_change = task.get('current_size_change', "-")
                original_completed_date = task.get('current_completed_date', "")
                original_status = task.get('current_status', t('status_completed'))
                check_vmaf = bool(task.get('check_vmaf', True))
                check_psnr = bool(task.get('check_psnr', True))
                if not check_vmaf and not check_psnr:
                    check_vmaf = True
        
                pending_vmaf = bool(check_vmaf)
                pending_psnr = bool(check_psnr)
        
                # Debug: logoljuk, hogy megkaptuk a feladatot
                with console_redirect(svt_logger):
                    print(f"\n{t('log_vmaf_task_received').format(filename=video_path.name)}")
                    print(f"   Output: {output_file.name}")
                    print(f"   VMAF: {check_vmaf}, PSNR: {check_psnr}")
            except Exception as _vmaf_task_err:
                with console_redirect(svt_logger):
                    print(f"[ERROR] VMAF task extraction failed: {_vmaf_task_err}")
                VMAF_QUEUE.task_done()
                continue
    
            # Újraellenőrizzük az azonnali leállítást
            if STOP_EVENT.is_set():
                waiting_status = self._get_vmaf_waiting_status_text(check_vmaf, check_psnr)
                # Tree értékek a task-ból (GUI thread-ben lett lekérve)
                self.encoding_queue.put((
                    "update",
                    item_id,
                    waiting_status,
                    original_cq_str,
                    original_vmaf_str,
                    original_psnr_str,
                    "-",
                    orig_size_str,
                    original_new_size_str,
                    original_size_change,
                    original_completed_date
                ))
                self.encoding_queue.put(("tag", item_id, "pending"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                VMAF_QUEUE.task_done()
                VMAF_QUEUE.put(task)
                if hasattr(self, 'vmaf_processing_videos'):
                    self.vmaf_processing_videos.discard(video_path)
                schedule_vmaf_idle()
                break
            
            # Ellenőrizzük, hogy a fájlok léteznek-e
            if not video_path.exists() or not output_file.exists():
                # Tree értékek a task-ból (GUI thread-ben lett lekérve)
                self.encoding_queue.put(("update", item_id, t('status_file_missing'), "-", "-", "-", "-", orig_size_str, "-", "-", original_completed_date))
                self.encoding_queue.put(("tag", item_id, "failed"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                if hasattr(self, 'vmaf_processing_videos'):
                    self.vmaf_processing_videos.discard(video_path)
                VMAF_QUEUE.task_done()
                continue
            
            # KRITIKUS FIX: Reset logger state BEFORE VMAF calculation!
            # Az encoding után a logger "dirty" state-ben marad (current_video_path beállítva az előző videóra)
            # Ez okozza hogy a VMAF output nem jelenik meg a konzolon!
            if hasattr(svt_logger, 'set_current_video_path'):
                svt_logger.set_current_video_path(video_path)
            
            # KRITIKUS: Logger stack törlése a manuális átkódolás után, hogy a logger használható legyen
            # Ez biztosítja, hogy ne maradjon beakadt logger a stack-ben
            try:
                from .core_paths_tools_logging import STDOUT_ROUTER
                STDOUT_ROUTER.clear_all_loggers()
            except (ImportError, AttributeError):
                # Import error - non-critical
                pass  # Csendes hiba - ne zavarjuk meg a folyamatot
            
            # Párhuzamosítás engedélyezése: KIHAGYJUK a CPU_WORKER_LOCK-ot
            # Ez lehetővé teszi, hogy több VMAF worker fusson párhuzamosan (a felhasználó által beállított limitszámig)
            if True:
                with console_redirect(svt_logger):
                    print(t('log_cpu_lock_acquired'))
                # KRITIKUS: A lock-on belül dolgozunk, hogy egyesével történjen a feldolgozás
                # és csak az aktív videó legyen "folyamatban" státuszban
                # KRITIKUS: NE használjuk a self.tree.item()-t közvetlenül worker threadben (nem thread-safe)!
                # A tree értékeket a task-ból használjuk, amit a GUI thread-ben (thread-safe módon) kértek le
                
                # KRITIKUS: Ha a méret vagy változás "-", számoljuk ki az output fájlból!
                if (original_new_size_str == "-" or original_size_change == "-") and output_file.exists():
                    try:
                        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                        if original_new_size_str == "-" and new_size_mb > 0:
                            original_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        if original_size_change == "-" and orig_size_mb > 0:
                            original_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                    except (OSError, IOError, PermissionError):
                        # File delete error - non-critical
                        pass  # Ha nem sikerül kiszámolni, az eredeti értékeket használjuk
                
                # Ellenőrizzük, hogy az azonnali leállítás nincs-e már beállítva
                if STOP_EVENT.is_set():
                    # Azonnali leállítás már be van állítva, ne kezdjük el a VMAF számítást
                    # Eredeti értékek visszaállítása
                    self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    if hasattr(self, 'vmaf_processing_videos'):
                        self.vmaf_processing_videos.discard(video_path)
                    VMAF_QUEUE.task_done()
                    continue
                
                # Státusz beállítása "VMAF számítás folyamatban..."-ra
                # KRITIKUS: NE használjuk a self.tree.item()-t worker threadben (nem thread-safe)!
                # A státusz frissítést queue-n keresztül végezzük
                with console_redirect(svt_logger):
                    print(t('log_status_update_vmaf'))
                self.encoding_queue.put(("update", item_id, t('status_vmaf_calculating'), original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, "-"))
                self.encoding_queue.put(("tag", item_id, "encoding"))
                
                # Kezdési időpont tárolása
                self.encoding_start_times[item_id] = time.time()
                
                # KRITIKUS: Mentjük el az értékeket a progress_callback számára
                # NE olvassuk be a tree-t a worker threadben (nem thread-safe)!
                callback_cq_str = original_cq_str
                callback_vmaf_str = original_vmaf_str
                callback_psnr_str = original_psnr_str
                callback_orig_size_str = orig_size_str
                callback_new_size_str = original_new_size_str
                callback_size_change = original_size_change
                # KRITIKUS: NE hívjuk meg a get_video_info-t itt, mert elakadhat!
                # A calculate_full_vmaf-ből kapjuk meg a videó információt a progress callback-en keresztül
                video_duration_seconds = None  # Kezdetben None, majd a progress callback frissíti
                total_duration_str = None
                metric_results = {'vmaf': None, 'psnr': None}
                metadata_updated_once = False
                current_progress_message = "-"
                current_status_display = t('status_vmaf_calculating')
    
                def update_metadata_partial():
                    nonlocal metadata_updated_once
                    if metric_results['vmaf'] is None:
                        return
                    with console_redirect(svt_logger):
                        update_video_metadata_vmaf(output_file, metric_results['vmaf'], psnr_value=metric_results['psnr'], logger=svt_logger)
                    metadata_updated_once = True
    
                def push_partial_update():
                    completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                    self.encoding_queue.put(("update", item_id, current_status_display, callback_cq_str, callback_vmaf_str, callback_psnr_str, current_progress_message, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
    
                def metric_done_callback(metric_name, value):
                    nonlocal callback_vmaf_str, callback_psnr_str, current_status_display, pending_vmaf, pending_psnr
                    metric_upper = (metric_name or "").upper()
                    if metric_upper == 'VMAF':
                        metric_results['vmaf'] = value
                        callback_vmaf_str = format_metric_value(value)
                        pending_vmaf = False
                        update_metadata_partial()
                        if check_psnr:
                            current_status_display = t('status_psnr_only')
                        push_partial_update()
                    elif metric_upper in ('PSNR', 'XPSNR'):
                        metric_results['psnr'] = value
                        callback_psnr_str = format_metric_value(value)
                        pending_psnr = False
                        if metric_results['vmaf'] is None:
                            try:
                                _, output_vmaf_meta, _, _, _, _, _, _, _, _ = get_output_file_info(output_file)
                                if output_vmaf_meta is not None:
                                    metric_results['vmaf'] = output_vmaf_meta
                                    callback_vmaf_str = format_metric_value(output_vmaf_meta)
                            except (OSError, IOError, ValueError, AttributeError):
                                # File or calculation error - non-critical
                                pass
                        update_metadata_partial()
                        push_partial_update()
    
                def progress_callback(msg):
                    nonlocal current_status_display, current_progress_message
                    # Ellenőrizzük, hogy az azonnali leállítás nincs-e beállítva
                    if STOP_EVENT.is_set():
                        return
                    # KRITIKUS: NE olvassuk a tree-t (nem thread-safe worker threadben)!
                    # Használjuk az elmentett értékeket
                    status = t('status_vmaf_calculating')
                    progress_display = "-"
                    completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
    
                    if isinstance(msg, dict) and msg.get('type') == 'abav1_progress':
                        metric_name = msg.get('metric')
                        if metric_name == 'VMAF':
                            status = t('status_vmaf_only')
                        elif metric_name in ('XPSNR', 'PSNR'):
                            status = t('status_psnr_only')
                        percent = msg.get('percent')
                        eta_seconds = msg.get('eta_seconds')
                        duration_for_calc = video_duration_seconds if video_duration_seconds else msg.get('duration_seconds')
                        elapsed_seconds = msg.get('elapsed_seconds')
                        if elapsed_seconds is None and percent is not None and duration_for_calc:
                            elapsed_seconds = max(0.0, min(duration_for_calc, duration_for_calc * (percent / 100.0)))
                        elapsed_str = format_seconds_hms(elapsed_seconds) if elapsed_seconds is not None else None
                        total_str = total_duration_str or format_seconds_hms(duration_for_calc)
                        if elapsed_str and total_str:
                            progress_display = f"{elapsed_str} / {total_str}"
                        elif percent is not None:
                            progress_display = f"{format_localized_number(percent, decimals=1)}%"
                        else:
                            progress_display = msg.get('text', "-")
    
                        if eta_seconds is not None:
                            try:
                                eta_seconds = float(max(0.0, eta_seconds))
                                estimated_end_datetime = datetime.fromtimestamp(time.time() + eta_seconds)
                                estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                                self.estimated_end_dates[item_id] = estimated_end_str
                                completed_date_to_use = estimated_end_str
                            except (ValueError, OverflowError):
                                completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                        else:
                            completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                    else:
                        progress_display = msg if isinstance(msg, str) else str(msg)
                        completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
    
                    current_status_display = status
                    current_progress_message = progress_display
                    self.encoding_queue.put(("update", item_id, status, callback_cq_str, callback_vmaf_str, callback_psnr_str, progress_display, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
                
                # KRITIKUS: A calculate_full_vmaf() hívás a CPU_WORKER_LOCK-on BELÜL történik,
                # hogy ne fusson egyszerre SVT-AV1 és VMAF számítás
                # De a VMAF_LOCK-ot kiengedjük, hogy ne blokkoljuk a státusz frissítéseket
                try:
                    # VMAF számítás SVT-AV1 konzolba (CPU-s worker)
                    with console_redirect(svt_logger):
                        print(f"\n{'='*80}")
                        print(f"{t('log_vmaf_test').format(filename=video_path.name)}")
                        print(f"{'='*80}")
                        print(t('log_calling_vmaf'))
                        print(f"   Reference (source): {vmaf_reference_path.name}")  # CHANGED: Use vmaf_reference_path!
                        print(f"   Encoded (output): {output_file.name}")
                        print(f"   Check VMAF: {check_vmaf}, Check PSNR: {check_psnr}")
                        print(f"   Files exist: ref={vmaf_reference_path.exists()}, enc={output_file.exists()}")
                    
                    # Check reference exists
                    if not vmaf_reference_path.exists() or not output_file.exists():
                        with console_redirect(svt_logger):
                            print(t('log_error_files_missing'))
                            if not vmaf_reference_path.exists():
                                print(f"   [ERROR] Reference missing: {vmaf_reference_path}")
                            if not output_file.exists():
                                print(f"   [ERROR] Encoded missing: {output_file}")
                        raise FileNotFoundError(f"VMAF files missing: ref={vmaf_reference_path.exists()}, enc={output_file.exists()}")
                    
                    with console_redirect(svt_logger):
                        print(t('log_files_verified'))
                        vmaf_result = calculate_full_vmaf(
                            vmaf_reference_path,  # CRITICAL: Use original source, NOT denoised master!
                            output_file,
                            progress_callback,
                            STOP_EVENT,
                            logger=svt_logger,
                            check_vmaf=check_vmaf,
                            check_psnr=check_psnr,
                            metric_done_callback=metric_done_callback
                        )
                        print(t('log_vmaf_returned').format(result=vmaf_result))
                        if vmaf_result:
                            vmaf_value, psnr_value = vmaf_result
                        else:
                            vmaf_value, psnr_value = (None, None)
                    
                    final_vmaf_value = metric_results['vmaf'] if metric_results['vmaf'] is not None else vmaf_value
                    final_psnr_value = metric_results['psnr'] if metric_results['psnr'] is not None else psnr_value
                    metrics_ok = True
                    if check_vmaf and final_vmaf_value is None:
                        metrics_ok = False
                    if check_psnr and final_psnr_value is None:
                        metrics_ok = False
                    
                    if metrics_ok:
                        with console_redirect(svt_logger):
                            print(f"[OK] VMAF/PSNR kalkuláció sikeres")
                            print(f"   Final VMAF: {final_vmaf_value}, Final PSNR: {final_psnr_value}")
                        
                        # Technical debug info for forceconsole
                        debug_print(f"[DEBUG] Metadata update state: already_updated={metadata_updated_once}")
                        
                        if not metadata_updated_once and final_vmaf_value is not None:
                            with console_redirect(svt_logger):
                                print(f"🔄 Metaadat frissítés...")
                                try:
                                    update_video_metadata_vmaf(output_file, final_vmaf_value, psnr_value=final_psnr_value, logger=svt_logger)
                                    metadata_updated_once = True
                                    print(f"[OK] Metaadat frissítés sikeres")
                                except Exception as meta_err:
                                    print(f"[ERROR] METAADAT FRISSÍTÉS HIBA: {meta_err}")
                                    # Technical error details to forceconsole
                                    debug_print(f"[DEBUG] Metadata update exception: {meta_err}")
                                    import traceback
                                    traceback.print_exc(file=sys.__stdout__)
                        
                        # Technical debug: file info reading
                        debug_print(f"[DEBUG] Reading output file info: {output_file.name}")
                        try:
                            output_cq_crf, output_vmaf_meta, output_psnr_meta, output_frame_count, output_file_size, output_modified_date, output_encoder_type, _, _, _ = get_output_file_info(output_file)
                            debug_print(f"[DEBUG] File info read successfully")
                        except Exception as info_err:
                            debug_print(f"[DEBUG] File info read FAILED: {info_err}")
                            import traceback
                            traceback.print_exc(file=sys.__stdout__)
                            # Use fallback values
                            output_cq_crf = None
                            output_vmaf_meta = None
                            output_psnr_meta = None
                            output_frame_count = None
                            output_file_size = None
                            output_modified_date = None
                            output_encoder_type = None
                        
                        # CQ/CRF érték - ha van a célfájlban, azt használjuk, különben az eredeti
                        final_cq_str = original_cq_str
                        if output_cq_crf is not None:
                            final_cq_str = str(output_cq_crf)
                        # If output CQ/CRF is unavailable, keep the original value.
                        
                        # Fájlméret és változás - ha van a célfájlban, azt használjuk
                        final_new_size_str = original_new_size_str
                        final_size_change = original_size_change
                        if output_file_size is not None:
                            new_size_mb = output_file_size / (1024**2)
                            final_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            
                            # Változás számítása, ha van eredeti méret
                            if orig_size_str and orig_size_str != "-" and 'MB' in orig_size_str:
                                try:
                                    orig_size_val = float(orig_size_str.replace(' MB', '').replace(',', '.'))
                                    change_percent = ((new_size_mb - orig_size_val) / orig_size_val) * 100 if orig_size_val > 0 else 0
                                    final_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                                except (ValueError, TypeError, ZeroDivisionError, AttributeError):
                                    pass
                        
                        # Táblázat frissítés - eredeti értékek visszaállítása + frissített VMAF
                        # Ha várakozó státusz volt, akkor a kódoló típusa alapján határozzuk meg a helyes "Kész" státuszt
                        final_status_code_hint = task.get('final_status_code')
                        status_hint = None
                        if final_status_code_hint:
                            localized_hint = status_code_to_localized(final_status_code_hint)
                            if localized_hint and normalize_status_to_code(localized_hint) == final_status_code_hint:
                                status_hint = localized_hint
                        
                        status_code = normalize_status_to_code(original_status)
                        if status_hint:
                            status = status_hint
                        elif status_code in ('vmaf_waiting', 'vmaf_psnr_waiting', 'psnr_waiting'):
                            # A fájl metadata-jából meghatározzuk a kódoló típusát
                            if output_encoder_type:
                                if output_encoder_type == 'nvenc':
                                    status = t('status_completed_nvenc')
                                elif output_encoder_type == 'svt-av1':
                                    status = t('status_completed_svt')
                                else:
                                    status = t('status_completed')
                            else:
                                status = t('status_completed')
                        else:
                            # Ha az original_status nem "VMAF ellenőrzésre vár...", akkor azt használjuk
                            status = original_status
                        # Eredeti completed_date visszaállítása (az átkódolás befejezési dátuma, nem a becsült VMAF befejezési idő)
                        # Ha van output_modified_date, azt használjuk
                        final_completed_date = output_modified_date if output_modified_date else original_completed_date
                        # VMAF és PSNR külön kezelése
                        if final_psnr_value is not None:
                            vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                            psnr_display = format_metric_value(final_psnr_value)
                        elif output_psnr_meta is not None:
                            if output_vmaf_meta is not None:
                                vmaf_display = format_metric_value(output_vmaf_meta)
                            elif final_vmaf_value is not None:
                                vmaf_display = format_metric_value(final_vmaf_value)
                            else:
                                vmaf_display = callback_vmaf_str
                            psnr_display = format_metric_value(output_psnr_meta)
                        else:
                            vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                            psnr_display = callback_psnr_str if callback_psnr_str not in ("", None, "-") else "-"
                        final_status_code = normalize_status_to_code(status)
                        if is_status_needs_check(status):
                            final_tag = "needs_check"
                        elif final_status_code == 'completed_copy':
                            final_tag = "completed_copy"
                        elif is_status_failed(status):
                            final_tag = "failed"
                        elif is_status_completed(status):
                            final_tag = "completed"
                        else:
                            final_tag = "pending"

                        self.encoding_queue.put(("update", item_id, status, final_cq_str, vmaf_display, psnr_display, "100%", orig_size_str, final_new_size_str, final_size_change, final_completed_date))
                        self.encoding_queue.put(("tag", item_id, final_tag))
                        
                        # Adatbázis frissítése VMAF/PSNR mérés befejezése után
                        # Számoljuk ki a new_size_mb-t és change_percent-et az adatbázis frissítéshez
                        # Használjuk az output_file_size-t (byte-ban), ne parse-oljunk MB stringet!
                        new_size_mb = None
                        change_percent = None
                        if output_file_size is not None:
                            new_size_mb = output_file_size / (1024**2)
                        elif final_new_size_str and final_new_size_str != "-" and 'MB' in final_new_size_str:
                            # Fallback: csak akkor parse-olunk, ha nincs output_file_size
                            try:
                                new_size_mb = float(final_new_size_str.replace(' MB', '').replace(',', '.'))
                            except (ValueError, TypeError):
                                pass
                        if final_size_change and final_size_change != "-" and '%' in final_size_change:
                            try:
                                change_percent = float(final_size_change.replace('%', '').replace('+', '').replace(',', '.'))
                            except (ValueError, TypeError):
                                pass
                        
                        if video_path:
                            # Háttérszálban frissítjük az adatbázist
                            def update_db_after_vmaf():
                                try:
                                    self.update_single_video_in_db(
                                        video_path, item_id, status, final_cq_str, 
                                        vmaf_display, psnr_display, orig_size_str, 
                                        new_size_mb, change_percent, final_completed_date
                                    )
                                except Exception as e:
                                    # Csendes hiba - ne zavarjuk meg a VMAF folyamatot
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(t('log_db_update_error').format(error=e, video=video_path) + "\n")
                                            LOG_WRITER.flush()
                                        except (OSError, IOError, ValueError, AttributeError):
                                            # File or calculation error - non-critical
                                            pass
                            
                            self._start_db_thread(update_db_after_vmaf, name="VmafResultDB", daemon=True)
                        
                        # CRITICAL: Cleanup denoised master AFTER VMAF finishes successfully
                        if denoised_master_to_cleanup and denoised_master_to_cleanup.exists():
                            from .core_preamble_and_imports import DEBUG_MODE
                            if DEBUG_MODE:
                                with console_redirect(svt_logger):
                                    print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve (VMAF kész): {denoised_master_to_cleanup.name}")
                            else:
                                try:
                                    master_size_mb = denoised_master_to_cleanup.stat().st_size / (1024 * 1024)
                                    denoised_master_to_cleanup.unlink()
                                    denoised_master_to_cleanup.with_suffix('.denoise_level').unlink(missing_ok=True)
                                    with console_redirect(svt_logger):
                                        print(f"[DEL] Zajszűrt mesterdarab törölve (VMAF kész): {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB felszabadítva)")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF kész, zajszűrt master takarítás | fájl: {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB)\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except Exception as e:
                                    with console_redirect(svt_logger):
                                        print(f"[WARN] Mesterdarab törlés hiba (VMAF után): {e}")
                        
                        # Kezdési időpont és becsült befejezési idő törlése
                        self.clear_encoding_times(item_id)
                        
                        # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                        if VMAF_QUEUE.empty():
                            schedule_vmaf_idle()
                        
                        with console_redirect(svt_logger):
                            if check_psnr and final_psnr_value is not None:
                                print(f"\n{t('log_vmaf_psnr_done').format(filename=video_path.name, vmaf=format_metric_value(final_vmaf_value) if final_vmaf_value is not None else '-', psnr=format_metric_value(final_psnr_value))}\n")
                            elif final_vmaf_value is not None and check_vmaf:
                                print(f"\n{t('log_vmaf_done').format(filename=video_path.name, vmaf=format_metric_value(final_vmaf_value))}\n")
                            elif check_psnr and final_psnr_value is not None:
                                print(f"\n{t('log_psnr_done').format(filename=video_path.name, psnr=format_metric_value(final_psnr_value))}\n")
                    else:
                        # VMAF/PSNR számítás hiba - eredeti értékek visszaállítása
                        self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                        self.encoding_queue.put(("tag", item_id, "failed"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        
                        # Kezdési időpont és becsült befejezési idő törlése
                        self.clear_encoding_times(item_id)
                        
                        # Cleanup denoised master (VMAF returned None)
                        if denoised_master_to_cleanup and denoised_master_to_cleanup.exists():
                            from .core_preamble_and_imports import DEBUG_MODE
                            if not DEBUG_MODE:
                                try:
                                    master_size_mb = denoised_master_to_cleanup.stat().st_size / (1024 * 1024)
                                    denoised_master_to_cleanup.unlink()
                                    denoised_master_to_cleanup.with_suffix('.denoise_level').unlink(missing_ok=True)
                                    with console_redirect(svt_logger):
                                        print(f"[DEL] Zajszűrt mesterdarab törölve (VMAF None): {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB felszabadítva)")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF sikertelen (None), zajszűrt master takarítás | fájl: {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB)\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                except (OSError, IOError, ValueError, AttributeError):
                                    pass
                        
                        # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                        if VMAF_QUEUE.empty():
                            schedule_vmaf_idle()
                        
                        with console_redirect(svt_logger):
                            print(f"\n{t('log_vmaf_calc_error').format(filename=video_path.name)}\n")
                
                except EncodingStopped:
                    pending_vmaf_flag = bool(pending_vmaf)
                    pending_psnr_flag = bool(pending_psnr)
                    if pending_vmaf_flag or pending_psnr_flag:
                        waiting_status = self._get_vmaf_waiting_status_text(pending_vmaf_flag, pending_psnr_flag)
                    else:
                        waiting_status = original_status
                    display_cq = callback_cq_str if callback_cq_str not in ("", None) else original_cq_str
                    display_vmaf = callback_vmaf_str if callback_vmaf_str not in ("", None) else original_vmaf_str
                    display_psnr = callback_psnr_str if callback_psnr_str not in ("", None) else original_psnr_str
                    self.encoding_queue.put((
                        "update",
                        item_id,
                        waiting_status,
                        display_cq,
                        display_vmaf,
                        display_psnr,
                        "-",
                        orig_size_str,
                        callback_new_size_str,
                        callback_size_change,
                        original_completed_date
                    ))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    stop_msg = (
                        f"\n{t('log_immediate_stop_vmaf_interrupted').format(filename=video_path.name)}\n"
                        if STOP_EVENT.is_set()
                        else f"\n{t('log_vmaf_interrupted').format(filename=video_path.name)}\n"
                    )
                    with console_redirect(svt_logger):
                        print(stop_msg)
                    if pending_vmaf_flag or pending_psnr_flag:
                        retry_task = dict(task)
                        retry_task['check_vmaf'] = pending_vmaf_flag
                        retry_task['check_psnr'] = pending_psnr_flag
                        VMAF_QUEUE.put(retry_task)
                    # Kezdési időpont és becsült befejezési idő törlése
                    self.clear_encoding_times(item_id)
                    
                    # Cleanup denoised master even if VMAF was interrupted
                    if denoised_master_to_cleanup and denoised_master_to_cleanup.exists():
                        from .core_preamble_and_imports import DEBUG_MODE
                        if DEBUG_MODE:
                            with console_redirect(svt_logger):
                                print(f"[STOP] DEBUG: Zajszűrt mesterdarab megőrizve (VMAF megszakítva): {denoised_master_to_cleanup.name}")
                        else:
                            try:
                                master_size_mb = denoised_master_to_cleanup.stat().st_size / (1024 * 1024)
                                denoised_master_to_cleanup.unlink()
                                denoised_master_to_cleanup.with_suffix('.denoise_level').unlink(missing_ok=True)
                                with console_redirect(svt_logger):
                                    print(f"[DEL] Zajszűrt mesterdarab törölve (VMAF megszakítva): {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB felszabadítva)")
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF megszakítva, zajszűrt master takarítás | fájl: {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB)\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except Exception as e:
                                with console_redirect(svt_logger):
                                    print(f"[WARN] Mesterdarab törlés hiba (VMAF megszakítás): {e}")
                    
                    # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                    if VMAF_QUEUE.empty():
                        schedule_vmaf_idle()
                    
                    if hasattr(self, 'vmaf_processing_videos'):
                        self.vmaf_processing_videos.discard(video_path)
                    VMAF_QUEUE.task_done()
                    # Ha azonnali leállítás van, kilépünk
                    if STOP_EVENT.is_set():
                        schedule_vmaf_idle()
                        break
                    continue
                except Exception as e:
                    # Hiba esetén eredeti értékek visszaállítása
                    self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    
                    # Kezdési időpont és becsült befejezési idő törlése
                    self.clear_encoding_times(item_id)
                    
                    # Cleanup denoised master even on error
                    if denoised_master_to_cleanup and denoised_master_to_cleanup.exists():
                        from .core_preamble_and_imports import DEBUG_MODE
                        if not DEBUG_MODE:
                            try:
                                master_size_mb = denoised_master_to_cleanup.stat().st_size / (1024 * 1024)
                                denoised_master_to_cleanup.unlink()
                                denoised_master_to_cleanup.with_suffix('.denoise_level').unlink(missing_ok=True)
                                with console_redirect(svt_logger):
                                    print(f"[DEL] Zajszűrt mesterdarab törölve (VMAF hiba): {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB felszabadítva)")
                                if LOG_WRITER:
                                    try:
                                        LOG_WRITER.write(f"[FILE_DELETE] Törlés | oka: VMAF hiba, zajszűrt master takarítás | fájl: {denoised_master_to_cleanup.name} ({master_size_mb:.1f} MB)\n")
                                        LOG_WRITER.flush()
                                    except Exception:
                                        pass
                            except (OSError, IOError, ValueError, AttributeError):
                                # File or calculation error - non-critical
                                pass
                    
                    # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                    if VMAF_QUEUE.empty():
                        schedule_vmaf_idle()
                    
                    with console_redirect(svt_logger):
                        print(f"\n{t('log_vmaf_test_error').format(error=e)}\n")
            
            if hasattr(self, 'vmaf_processing_videos'):
                self.vmaf_processing_videos.discard(video_path)
            
            # KRITIKUS: Clean up logger state AFTER VMAF task
            if hasattr(svt_logger, 'set_current_video_path'):
                svt_logger.set_current_video_path(None)
            
            VMAF_QUEUE.task_done()

            # Sima leállítás ellenőrzése a feladat után
            # THREAD-SAFETY FIX: Use get_encoding_state() for consistent access
            is_enc_after, _, graceful_stop_after = self.get_encoding_state()
            if graceful_stop_after and VMAF_QUEUE.empty():
                with console_redirect(svt_logger):
                    print(f"\n{t('log_graceful_stop_vmaf_done')}\n")
                schedule_vmaf_idle()
                break

            # Ha nincs több VMAF feladat és nincs aktív kódolás, gombok inaktiválása
            if VMAF_QUEUE.empty() and not is_enc_after:
                schedule_vmaf_idle()
                break

    def request_vmaf_test(self, video_path, item_id, check_vmaf=True, check_psnr=True):
        """Request VMAF/PSNR calculation for a specific video.
        
        Adds the request to the VMAF worker queue.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            check_vmaf: Whether to calculate VMAF.
            check_psnr: Whether to calculate PSNR.
        """
    
        output_file = self.video_to_output.get(video_path)
        if not output_file or not output_file.exists():
            messagebox.showerror(t('msg_error'), t('msg_output_not_found'))
            return
        
        if not check_vmaf and not check_psnr:
            check_vmaf = True  # Biztonsági okból legalább az egyiket futtatjuk
        
        # KRITIKUS: Ha STOP_EVENT be van állítva (az előző azonnali leállítás miatt), töröljük
        # Mert különben a VMAF/PSNR worker azonnal kilép
        # ThreadSafeEvent provides built-in atomicity, no external lock needed
        if STOP_EVENT.is_set():
            STOP_EVENT.clear()
        
        # VMAF/PSNR queue-ba helyezés
        # KRITIKUS: A tree értékeket itt kérjük le (GUI thread-ben), és beletesszük a task-ba
        # Így a worker thread-ben nem kell a tree-t elérni
        try:
            current_values = self.get_tree_values(item_id, min_length=11)
        except Exception as e:
            # KRITIKUS HIBA: Tree elérés GUI thread-ben is elakadhat!
            error_msg = t('log_tree_access_error').format(error=e) + "\n"
            error_msg += f"   item_id: {item_id}\n"
            error_msg += f"   video_path: {video_path}\n"
            print(error_msg, flush=True)
            import traceback
            traceback.print_exc()
            # Fallback: alapértelmezett értékek
            current_values = [''] * 11
            current_values[self.COLUMN_INDEX['orig_size']] = "-"
            current_values[self.COLUMN_INDEX['status']] = t('status_completed')
            current_values[self.COLUMN_INDEX['cq']] = "-"
            current_values[self.COLUMN_INDEX['vmaf']] = "-"
            current_values[self.COLUMN_INDEX['psnr']] = "-"
            current_values[self.COLUMN_INDEX['new_size']] = "-"
            current_values[self.COLUMN_INDEX['size_change']] = "-"
            current_values[self.COLUMN_INDEX['completed_date']] = ""
        
        vmaf_task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'orig_size_str': current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
            'current_cq_str': current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
            'current_vmaf_str': current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
            'current_psnr_str': current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
            'current_new_size_str': current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
            'current_size_change': current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
            'current_completed_date': current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else "",
            'current_status': current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else t('status_completed'),
            'check_vmaf': bool(check_vmaf),
            'check_psnr': bool(check_psnr),
            'type': 'vmaf',  # Task type marker for SVT worker routing
        }
        
        # CRITICAL FIX: Check if denoised master exists (for denoised videos)
        # If it exists, use it as VMAF reference instead of original source
        vmaf_reference_path = None
        # Fix: Use video_path.stem to match creation logic in svt_worker (filename_denoised_master.mkv)
        denoised_master_path = output_file.parent / f"{video_path.stem}_denoised_master.mkv"
        if denoised_master_path.exists():
            vmaf_reference_path = denoised_master_path
            from .core_preamble_and_imports import LOG_WRITER
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"[OK] Denoised master detected for manual VMAF: {denoised_master_path.name}\n")
                    LOG_WRITER.write(f"  Using denoised master as VMAF reference (not original source)\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, AttributeError, ValueError):
                    # Log writer error - non-critical
                    pass
        
        # LIST-BASED QUEUE: Add manual VMAF task to SVT queue
        # VMAF calculations now wait for an available SVT worker
        self.add_to_svt_queue(
            video_path=video_path,
            item_id=item_id,
            task_type='vmaf',
            is_manual=True,  # VMAF tasks are priority
            output_file=output_file,
            orig_size_str=vmaf_task['orig_size_str'],
            check_vmaf=bool(check_vmaf),
            check_psnr=bool(check_psnr),
            current_cq_str=vmaf_task['current_cq_str'],
            current_vmaf_str=vmaf_task['current_vmaf_str'],
            current_psnr_str=vmaf_task['current_psnr_str'],
            current_new_size_str=vmaf_task['current_new_size_str'],
            current_size_change=vmaf_task['current_size_change'],
            current_completed_date=vmaf_task['current_completed_date'],
            current_status=vmaf_task['current_status'],
            vmaf_reference_path=vmaf_reference_path,  # Pass denoised master if exists, None otherwise
            reason='manual_vmaf_test'
        )
        
        # Státusz frissítés
        # KRITIKUS: Tree elérés GUI thread-ben - exception kezeléssel
        try:
            current_values = self.tree.item(item_id, 'values')
        except Exception as e:
            # KRITIKUS HIBA: Tree elérés GUI thread-ben is elakadhat!
            error_msg = t('log_tree_access_error').format(error=e) + "\n"
            error_msg += f"   item_id: {item_id}\n"
            error_msg += f"   video_path: {video_path}\n"
            print(error_msg, flush=True)
            import traceback
            traceback.print_exc()
            # Fallback: alapértelmezett értékek
            current_values = [''] * 11
        
        cq_str = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
        psnr_str = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
        progress_str = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else "-"
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        new_size_str = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
        change_str = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        waiting_status = self._get_vmaf_waiting_status_text(vmaf_task['check_vmaf'], vmaf_task['check_psnr'])
        
        # DEBUG: Log VMAF queue operation
        from .core_preamble_and_imports import LOG_WRITER
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"\n{'='*60}\n")
                LOG_WRITER.write(f"[request_vmaf_test] Manual VMAF request\n")
                LOG_WRITER.write(f"  Video: {video_path.name}\n")
                LOG_WRITER.write(f"  item_id: {item_id}\n")
                LOG_WRITER.write(f"  Output: {output_file.name}\n")
                LOG_WRITER.write(f"  Check VMAF: {vmaf_task['check_vmaf']}, PSNR: {vmaf_task['check_psnr']}\n")
                LOG_WRITER.write(f"  Status: {waiting_status}\n")
                LOG_WRITER.write(f"  Sending GUI update (encoding_queue.put)...\n")
                LOG_WRITER.write(f"{'='*60}\n\n")
                LOG_WRITER.flush()
            except (ImportError, AttributeError):
                # Import error - non-critical
                pass
        
        self.encoding_queue.put(("update", item_id, waiting_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
        
        # CRITICAL FIX: Ensure check_encoding_queue() loop is running
        # If not encoding (loaded state), the queue processing won't run and GUI won't update!
        if not self.is_encoding:
            # Start the queue processing loop (it will process this update message)
            self.root.after(100, self.check_encoding_queue)
        
        # Ensure SVT workers are running (they handle both encoding and VMAF tasks now)
        self._ensure_svt_workers_running()

    def request_vmaf_test_multiple(self, item_ids):
        """VMAF/PSNR teszt kérése több videóra"""
        for item_id in item_ids:
            video_path = None
            for vid_path, vid_item_id in self.video_items.items():
                if vid_item_id == item_id:
                    video_path = vid_path
                    break
            
            if video_path:
                self.request_vmaf_test(video_path, item_id)

    def ensure_vmaf_worker_running(self):
        """Gondoskodik róla, hogy a VMAF/PSNR worker(ek) és a queue feldolgozás aktív legyen."""
        self._vmaf_stop_drained = False
        # ThreadSafeEvent provides built-in atomicity, no external lock needed
        if STOP_EVENT.is_set():
            STOP_EVENT.clear()
        
        # Determine how many workers to start (match SVT worker count)
        svt_worker_count = self.get_configured_svt_workers() if hasattr(self, 'get_configured_svt_workers') else 1
        
        # Initialize thread list if needed
        if not hasattr(self, 'vmaf_worker_threads'):
            self.vmaf_worker_threads = []
            
        # Clean up dead threads
        self.vmaf_worker_threads = [t for t in self.vmaf_worker_threads if t.is_alive()]
        
        # Start new threads if needed
        active_workers = len(self.vmaf_worker_threads)
        if active_workers < svt_worker_count:
            workers_to_start = svt_worker_count - active_workers
            start_idx = active_workers
            for i in range(workers_to_start):
                 worker_idx = start_idx + i
                 try:
                     thread = threading.Thread(target=self.vmaf_worker, args=(worker_idx,), daemon=True)
                     thread.start()
                     self.vmaf_worker_threads.append(thread)
                 except Exception as e:
                     print(f"Failed to start VMAF worker {worker_idx}: {e}")
        
        if self.vmaf_worker_threads:
            self.vmaf_worker_active = True

        self.immediate_stop_button.config(state=tk.NORMAL)
        # KRITIKUS: Ha van feladat a VMAF_QUEUE-ban, akkor be kell állítani az is_encoding flaget,
        # hogy a worker ne lépjen ki, még akkor sem, ha nincs más aktív kódolás
        # THREAD-SAFETY FIX: Use helper method for lock-protected state access
        is_enc, _, _ = self.get_encoding_state()
        if not is_enc and not VMAF_QUEUE.empty():
            self.set_encoding_state(is_encoding=True)
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.check_encoding_queue()

    def _get_vmaf_waiting_status_text(self, check_vmaf, check_psnr):
        if check_vmaf and check_psnr:
            return t('status_vmaf_psnr_waiting')
        if check_vmaf:
            return t('status_vmaf_waiting')
        return t('status_psnr_waiting')

    def _on_vmaf_worker_finished(self):
        """VMAF/PSNR worker leállása után frissíti az UI-t."""
        # Check if ALL vmaf workers are done
        vmaf_still_running = False
        if hasattr(self, 'vmaf_worker_threads'):
            self.vmaf_worker_threads = [t for t in self.vmaf_worker_threads if t.is_alive()]
            if self.vmaf_worker_threads:
                vmaf_still_running = True
        
        self.vmaf_worker_active = vmaf_still_running
        self._reset_encoding_ui_if_idle()

    def _confirm_setting_change_during_encoding(self, var, new_value, old_value):
        """Átkódolás közbeni beállítás-módosítás megerősítése.
        
        Ha kódolás fut, figyelmeztető ablakot jelenít meg.
        
        Args:
            var: A tk változó (pl. self.min_vmaf)
            new_value: Az új érték
            old_value: A régi érték
            
        Returns:
            bool: True ha a változtatás elfogadásra került, False ha visszaállítás történt
        """
        # Csak akkor kérünk megerősítést, ha kódolás fut
        if not getattr(self, 'is_encoding', False):
            return True
            
        # Ha az érték ugyanaz, nincs szükség megerősítésre
        if new_value == old_value:
            return True
            
        # Figyelmeztetés megjelenítése
        result = messagebox.askokcancel(
            t('settings_change_during_encoding_title'),
            t('settings_change_during_encoding_message')
        )
        
        if result:
            # OK: Elfogadja a változtatást
            return True
        else:
            # Mégse: Visszaállítja az előző értéket
            var.set(old_value)
            return False

    def update_vmaf_label(self, value):
        rounded_value = round(float(value) * 2) / 2
        # Előző érték mentése megerősítés előtt
        old_value = getattr(self, '_last_min_vmaf_value', rounded_value)
        if not hasattr(self, '_last_min_vmaf_value'):
            self._last_min_vmaf_value = rounded_value
        
        # Ha kódolás fut és az érték ténylegesen változott
        if rounded_value != old_value:
            if not self._confirm_setting_change_during_encoding(self.min_vmaf, rounded_value, old_value):
                # Visszaállítás: frissítsük a labelt is
                self.vmaf_value_label.config(text=format_localized_number(old_value, decimals=1))
                return
        
        # Változtatás elfogadva
        self._last_min_vmaf_value = rounded_value
        self.min_vmaf.set(rounded_value)
        self.vmaf_value_label.config(text=format_localized_number(rounded_value, decimals=1))

    def update_vmaf_step_label(self, value):
        # Round to nearest 0.05 (multiply by 20, round, divide by 20)
        rounded_value = round(float(value) * 20) / 20
        old_value = getattr(self, '_last_vmaf_step_value', rounded_value)
        if not hasattr(self, '_last_vmaf_step_value'):
            self._last_vmaf_step_value = rounded_value
        
        if rounded_value != old_value:
            if not self._confirm_setting_change_during_encoding(self.vmaf_step, rounded_value, old_value):
                self.vmaf_step_value_label.config(text=format_localized_number(old_value, decimals=2))
                return
        
        self._last_vmaf_step_value = rounded_value
        self.vmaf_step.set(rounded_value)
        self.vmaf_step_value_label.config(text=format_localized_number(rounded_value, decimals=2))

    # NOTE: update_max_encoded_label moved to SettingsControlMixin (takes precedence in MRO)
    # NOTE: update_resize_label moved to SettingsControlMixin (takes precedence in MRO)
    # NOTE: update_svt_preset_label moved to SettingsControlMixin (takes precedence in MRO)
    # NOTE: update_nvenc_workers_label moved to SettingsControlMixin (takes precedence in MRO)
    # See gui_settings_control.py for the consolidated implementations.

    def update_crf_increment_label(self, value):
        """Update the CRF increment slider label."""
        int_value = int(round(float(value)))
        old_value = getattr(self, '_last_crf_increment_value', int_value)
        if not hasattr(self, '_last_crf_increment_value'):
            self._last_crf_increment_value = int_value
        
        if int_value != old_value:
            if not self._confirm_setting_change_during_encoding(self.crf_increment, int_value, old_value):
                self.crf_increment_value_label.config(text=str(old_value))
                return
        
        self._last_crf_increment_value = int_value
        self.crf_increment.set(int_value)
        self.crf_increment_value_label.config(text=str(int_value))

    def _on_audio_compression_change(self):
        """Handler az audio compression checkbox változásához."""
        new_value = self.audio_compression_enabled.get()
        old_value = getattr(self, '_last_audio_compression_enabled_value', new_value)
        if not hasattr(self, '_last_audio_compression_enabled_value'):
            self._last_audio_compression_enabled_value = new_value
        
        # Ha kódolás fut és az érték változott
        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.audio_compression_enabled, new_value, old_value):
                # Mégse: visszaállítás
                return
        
        # Változtatás elfogadva
        self._last_audio_compression_enabled_value = new_value
        self._save_settings_debounced()

    def _on_deband_change(self):
        """Handler a deband checkbox változásához."""
        new_value = self.deband_enabled.get()
        old_value = getattr(self, '_last_deband_enabled_value', new_value)
        if not hasattr(self, '_last_deband_enabled_value'):
            self._last_deband_enabled_value = new_value

        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.deband_enabled, new_value, old_value):
                return

        self._last_deband_enabled_value = new_value
        self.current_deband_enabled = bool(new_value)
        self._save_settings_debounced()

    def _on_force_8bit_denoised_master_change(self):
        """Handler for the 8-bit denoised master test mode checkbox."""
        new_value = self.force_8bit_denoised_master.get()
        old_value = getattr(self, '_last_force_8bit_denoised_master_value', new_value)
        if not hasattr(self, '_last_force_8bit_denoised_master_value'):
            self._last_force_8bit_denoised_master_value = new_value

        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.force_8bit_denoised_master, new_value, old_value):
                return

        self._last_force_8bit_denoised_master_value = new_value
        self.current_force_8bit_denoised_master = bool(new_value)
        self._save_settings_debounced()

    def _on_skip_av1_change(self):
        """Handler a skip AV1 checkbox változásához."""
        new_value = self.skip_av1_files.get()
        old_value = getattr(self, '_last_skip_av1_value', new_value)
        if not hasattr(self, '_last_skip_av1_value'):
            self._last_skip_av1_value = new_value
        
        # Ha kódolás fut és az érték változott
        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.skip_av1_files, new_value, old_value):
                # Mégse: visszaállítás
                return
        
        # Változtatás elfogadva
        self._last_skip_av1_value = new_value
        self._save_settings_debounced()

    def _on_auto_vmaf_psnr_change(self):
        """Handler az auto VMAF/PSNR checkbox változásához."""
        new_value = self.auto_vmaf_psnr.get()
        old_value = getattr(self, '_last_auto_vmaf_psnr_value', new_value)
        if not hasattr(self, '_last_auto_vmaf_psnr_value'):
            self._last_auto_vmaf_psnr_value = new_value
        
        # Ha kódolás fut és az érték változott
        if new_value != old_value and getattr(self, 'is_encoding', False):
            if not self._confirm_setting_change_during_encoding(self.auto_vmaf_psnr, new_value, old_value):
                # Mégse: visszaállítás
                return
        
        # Változtatás elfogadva
        self._last_auto_vmaf_psnr_value = new_value
        self._save_settings_debounced()

    # NOTE: _ensure_svt_workers_running removed - defined in EncodingControlMixin (takes precedence in MRO)
