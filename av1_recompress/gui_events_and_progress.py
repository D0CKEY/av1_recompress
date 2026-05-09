from .gui_imports import *
from .gui_shared import *

class EventsAndProgressMixin:
    def _write_user_interaction_log(self, message):
        """Write a user interaction audit entry to av1_recompress.log."""
        try:
            writer = get_log_writer()
            if not writer:
                return
            if hasattr(writer, 'closed') and writer.closed:
                return
            writer.write(f"[UI] {message}\n")
            writer.flush()
        except Exception:
            # Interaction logging must never break UI event handling.
            pass

    def _safe_interaction_preview(self, value, limit=120):
        """Normalize and truncate long values for compact log lines."""
        if value is None:
            return ""
        try:
            text = str(value).replace("\r", "\\r").replace("\n", "\\n")
        except Exception:
            return ""
        if len(text) > limit:
            return text[:limit] + "...(truncated)"
        return text

    def _describe_interaction_widget(self, widget):
        """Return a compact widget description for audit logging."""
        if widget is None:
            return "widget=<none>"

        parts = []
        try:
            parts.append(f"class={widget.winfo_class()}")
        except Exception:
            parts.append("class=<unknown>")
        try:
            parts.append(f"path={widget}")
        except Exception:
            pass

        # Optional text/label metadata for actionable controls.
        for attr_name in ("text", "label"):
            try:
                attr_val = widget.cget(attr_name)
                if attr_val:
                    parts.append(f"{attr_name}='{self._safe_interaction_preview(attr_val, 80)}'")
                    break
            except Exception:
                continue

        # Control-specific state snapshot.
        try:
            widget_class = widget.winfo_class()
        except Exception:
            widget_class = ""

        try:
            if widget_class in ("TCombobox", "Combobox"):
                parts.append(f"value='{self._safe_interaction_preview(widget.get(), 120)}'")
            elif widget_class in ("Entry", "TEntry"):
                # Keep privacy/noise under control: length only for free-text fields.
                current_text = str(widget.get()) if hasattr(widget, "get") else ""
                parts.append(f"len={len(current_text)}")
            elif widget_class in ("Text", "ScrolledText"):
                try:
                    current_text = str(widget.get("1.0", "end-1c"))
                    parts.append(f"len={len(current_text)}")
                except Exception:
                    pass
            elif widget_class == "Treeview":
                if hasattr(widget, "selection"):
                    parts.append(f"selected={len(widget.selection())}")
            elif widget_class == "TNotebook":
                try:
                    selected = widget.select()
                    parts.append(f"tab={self._safe_interaction_preview(selected, 120)}")
                except Exception:
                    pass
            elif widget_class in ("Checkbutton", "TCheckbutton", "Radiobutton", "TRadiobutton"):
                try:
                    var_name = widget.cget("variable")
                    if var_name:
                        parts.append(f"state={self._safe_interaction_preview(widget.getvar(var_name), 40)}")
                except Exception:
                    pass
        except Exception:
            pass

        return ", ".join(parts)

    def _on_global_user_event(self, event, event_name):
        """Global GUI interaction logger callback."""
        try:
            if is_app_closing():
                return
            widget = getattr(event, "widget", None)
            widget_desc = self._describe_interaction_widget(widget)

            details = [f"type={event_name}", widget_desc]
            if event_name == "mouse_release":
                num = getattr(event, "num", None)
                x_root = getattr(event, "x_root", None)
                y_root = getattr(event, "y_root", None)
                if num is not None:
                    details.append(f"button={num}")
                if x_root is not None and y_root is not None:
                    details.append(f"screen=({x_root},{y_root})")

                # Treeview-specific click precision: row/column/cell details.
                try:
                    widget_class = widget.winfo_class() if widget is not None else ""
                except Exception:
                    widget_class = ""
                if widget_class == "Treeview" and widget is not None:
                    try:
                        x = getattr(event, "x", None)
                        y = getattr(event, "y", None)
                        if x is not None and y is not None:
                            region = widget.identify_region(x, y)
                            row_id = widget.identify_row(y)
                            col_token = widget.identify_column(x)
                            details.append(f"tree_region={region}")
                            if row_id:
                                details.append(f"tree_item={row_id}")
                            if col_token:
                                details.append(f"tree_col_token={col_token}")

                            # Resolve symbolic column name from token.
                            col_name = col_token
                            try:
                                if col_token and col_token.startswith("#"):
                                    if col_token == "#0":
                                        col_name = "#0"
                                    else:
                                        col_idx = int(col_token[1:]) - 1
                                        cols = list(widget["columns"]) if "columns" in widget.keys() else []
                                        if 0 <= col_idx < len(cols):
                                            col_name = str(cols[col_idx])
                                details.append(f"tree_col={col_name}")
                            except Exception:
                                pass

                            if row_id:
                                # Include selected item count to see multi-select context.
                                try:
                                    details.append(f"tree_selected={len(widget.selection())}")
                                except Exception:
                                    pass

                                # Include compact row metadata.
                                try:
                                    row_values = list(widget.item(row_id, 'values') or [])
                                    row_tags = widget.item(row_id, 'tags') or ()
                                    details.append(f"tree_tags={','.join(row_tags) if row_tags else '-'}")
                                    if hasattr(self, "COLUMN_INDEX"):
                                        status_idx = self.COLUMN_INDEX.get("status")
                                        video_idx = self.COLUMN_INDEX.get("video_name")
                                        if video_idx is not None and video_idx < len(row_values):
                                            details.append(f"tree_video='{self._safe_interaction_preview(row_values[video_idx], 100)}'")
                                        if status_idx is not None and status_idx < len(row_values):
                                            details.append(f"tree_status='{self._safe_interaction_preview(row_values[status_idx], 100)}'")
                                except Exception:
                                    pass

                                # Include clicked cell value where possible.
                                try:
                                    if col_token == "#0":
                                        cell_value = widget.item(row_id, 'text')
                                    elif col_token and col_token.startswith("#"):
                                        col_idx = int(col_token[1:]) - 1
                                        row_values = list(widget.item(row_id, 'values') or [])
                                        cell_value = row_values[col_idx] if 0 <= col_idx < len(row_values) else ""
                                    else:
                                        cell_value = ""
                                    if cell_value != "":
                                        details.append(f"tree_cell='{self._safe_interaction_preview(cell_value, 120)}'")
                                except Exception:
                                    pass

                                # Best-effort source file name resolution.
                                try:
                                    path_resolver = getattr(self, "_get_video_path_by_item", None)
                                    if callable(path_resolver):
                                        video_path = path_resolver(row_id)
                                        if video_path is not None:
                                            details.append(f"tree_source='{Path(video_path).name}'")
                                except Exception:
                                    pass
                    except Exception:
                        pass
            elif event_name == "key_release":
                keysym = getattr(event, "keysym", None)
                keycode = getattr(event, "keycode", None)
                state = getattr(event, "state", None)
                if keysym is not None:
                    details.append(f"keysym={keysym}")
                if keycode is not None:
                    details.append(f"keycode={keycode}")
                if state is not None:
                    details.append(f"state={state}")

            self._write_user_interaction_log(" | ".join(details))
        except Exception:
            # Never propagate errors from event logging callbacks.
            pass

    def setup_user_interaction_logging(self):
        """Install global GUI event bindings to audit user interactions."""
        if getattr(self, "_user_interaction_logging_enabled", False):
            return

        if not hasattr(self, "root") or self.root is None:
            return

        event_bindings = (
            ("<ButtonRelease>", "mouse_release"),
            ("<KeyRelease>", "key_release"),
            ("<<ComboboxSelected>>", "combobox_selected"),
            ("<<TreeviewSelect>>", "treeview_select"),
            ("<<TreeviewOpen>>", "treeview_open"),
            ("<<TreeviewClose>>", "treeview_close"),
            ("<<NotebookTabChanged>>", "notebook_tab_changed"),
        )

        try:
            for sequence, event_name in event_bindings:
                self.root.bind_all(
                    sequence,
                    lambda event, name=event_name: self._on_global_user_event(event, name),
                    add="+"
                )
            self._user_interaction_logging_enabled = True
            self._write_user_interaction_log("Global user interaction logging enabled")
        except Exception as e:
            self._write_user_interaction_log(f"[WARN] Failed to initialize interaction logging: {e}")

    def update_estimated_end_time_from_progress(self, item_id, progress_msg):
        """Becsült befejezési idő számítása - mintavételezés 5 másodperces átlagolással (frame szám alapján számolódik)"""
        if not progress_msg or " / " not in progress_msg:
            return

        try:
            progress_parts = progress_msg.split(" / ")
            if len(progress_parts) != 2:
                return

            elapsed_str = progress_parts[0].strip()
            total_str = progress_parts[1].strip()
            elapsed_parts = elapsed_str.split(":")
            total_parts = total_str.split(":")

            if len(elapsed_parts) != 3 or len(total_parts) != 3:
                return

            elapsed_seconds = int(elapsed_parts[0]) * 3600 + int(elapsed_parts[1]) * 60 + int(elapsed_parts[2])
            total_seconds = int(total_parts[0]) * 3600 + int(total_parts[1]) * 60 + int(total_parts[2])

            if elapsed_seconds <= 0 or total_seconds <= 0 or item_id not in self.encoding_start_times:
                return

            start_time = self.encoding_start_times[item_id]
            current_time = time.time()
            elapsed_time = current_time - start_time

            if elapsed_time <= 0:
                return

            remaining_video_seconds = total_seconds - elapsed_seconds
            if remaining_video_seconds <= 0:
                return

            encoding_speed = elapsed_seconds / elapsed_time
            if encoding_speed <= 0:
                return

            remaining_encoding_time = remaining_video_seconds / encoding_speed
            estimated_end_time = current_time + remaining_encoding_time

            # Inicializálás, ha még nem létezik az eta_samples dict
            if not hasattr(self, 'eta_samples'):
                self.eta_samples = {}

            # Mintavételezés: tároljuk a számított ETA-t timestamp-pel, de NEM frissítjük azonnal a GUI-t
            # A GUI frissítést a timer végzi 5 másodpercenként az átlagolt értékkel
            if item_id not in self.eta_samples:
                self.eta_samples[item_id] = []

            # Hozzáadjuk az új mintát (timestamp, estimated_end_timestamp)
            self.eta_samples[item_id].append((current_time, estimated_end_time))

            # Régi minták törlése (5 másodpercnél régebbiek)
            cutoff_time = current_time - 5.0
            self.eta_samples[item_id] = [(ts, eta) for ts, eta in self.eta_samples[item_id] if ts > cutoff_time]

        except (tk.TclError, KeyError, AttributeError, IndexError, queue.Full, ValueError, TypeError):
            pass

    def clear_encoding_times(self, item_id):
        """Clear start time and estimated end time for a video.

        Args:
            item_id: Treeview item ID.
        """
        if item_id in self.encoding_start_times:
            del self.encoding_start_times[item_id]
        if item_id in self.estimated_end_dates:
            del self.estimated_end_dates[item_id]
        # ETA minták törlése
        if hasattr(self, 'eta_samples') and item_id in self.eta_samples:
            del self.eta_samples[item_id]

    def start_estimated_end_timer(self):
        """Timer indítása a becsült befejezési idő frissítéséhez (5 másodpercenként, 5 másodperces mintavételi átlagolással)"""
        def update_estimated_end_times():
            if not hasattr(self, 'encoding_start_times'):
                self.encoding_start_times = {}
            
            current_time = time.time()
            items_to_remove = []
            
            for item_id, start_time in list(self.encoding_start_times.items()):
                try:
                    current_values = self.tree.item(item_id, 'values')
                    if len(current_values) < len(self.COLUMN_INDEX):
                        continue
                    
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    progress = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else ""
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    
                    # Ha kész vagy sikertelen, töröljük a kezdési időt
                    if is_status_completed(status) or is_status_failed(status):
                        items_to_remove.append(item_id)
                        continue

                    # Zajszűrés alatt ne számoljunk becsült befejezési időt, mert a folyamat sebessége nem lineáris
                    if normalize_status_to_code(status) == 'denoising':
                         continue

                    
                    # Ha a progress "100%" vagy hasonló, akkor a kódolás befejeződött vagy éppen befejeződik
                    # Ne írjuk felül a completed_date-t, mert az már a valódi befejezési dátum lehet
                    progress_stripped = progress.strip() if progress else ""
                    is_percentage_complete = False
                    if progress_stripped:
                        if progress_stripped == "100%":
                            is_percentage_complete = True
                        elif progress_stripped.endswith("%"):
                            # Ellenőrizzük, hogy százalékos érték-e (pl. "99.5%")
                            try:
                                percent_value = float(progress_stripped.replace("%", ""))
                                if percent_value >= 99.0:  # 99% felett tekintsük befejezettnek
                                    is_percentage_complete = True
                            except ValueError:
                                pass
                    
                    if is_percentage_complete:
                        continue
                    
                    # VMAF/PSNR számítás során is frissítsük a becsült befejezési időt
                    # (a status_vmaf_calculating státusz esetén is)
                    is_vmaf_calculating = (status == t('status_vmaf_calculating'))
                    
                    # Ha VMAF/PSNR számítás folyamatban van, de nincs még progress információ,
                    # akkor az első 10 másodpercben "-" jelenjen meg, majd 10 másodperc után kezdjen el számolni
                    # VMAF/PSNR számítás során MINDIG frissítsük a becsült befejezési időt (az átkódolás dátuma helyett)
                    if is_vmaf_calculating and (not progress or progress == "-" or " / " not in progress):
                        # Eltelt idő a kezdés óta
                        elapsed_time = current_time - start_time
                        
                        # Az első 10 másodpercben "-" jelenjen meg
                        if elapsed_time < 10:
                            # "-" beállítása (ne frissítsük, ha már "-" van)
                            if completed_date != "-":
                                self.encoding_queue.put_nowait(("update", item_id, status,
                                                        current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                        current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                        current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                        progress if progress else "-",
                                                        current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                        "-"))
                        else:
                            # 10 másodperc után számoljunk egy kezdeti becsült befejezési időt
                            # Kezdeti becslés: feltételezzük, hogy a VMAF számítás még legalább 2x annyi ideig tart
                            # (ez egy konzervatív becslés, amíg nem jön progress információ)
                            # 10 másodpercenként aktualizáljuk a becsült VMAF újraellenőrzés befejezési dátumidő pontot
                            estimated_total_time = elapsed_time * 3  # 3x az eltelt idő
                            estimated_end_time = start_time + estimated_total_time
                            
                            # Becsült befejezési idő formázása
                            estimated_end_datetime = datetime.fromtimestamp(estimated_end_time)
                            estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Tároljuk a becsült befejezési időt
                            self.estimated_end_dates[item_id] = estimated_end_str
                            
                            # VMAF számítás során mindig frissítsük a becsült befejezési időt
                            # (még akkor is, ha a completed_date dátum formátumú - az átkódolás dátuma)
                            # 10 másodpercenként aktualizáljuk a becsült VMAF újraellenőrzés befejezési dátumidő pontot
                            self.encoding_queue.put_nowait(("update", item_id, status,
                                                    current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                    current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                    current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                    progress if progress else "-",
                                                    current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                    current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                    current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                    estimated_end_str))
                    
                    # 5 másodperces mintavételi ablakból átlagolt ETA frissítés
                    # Ha van progress információ és van eta_samples, használjuk az átlagolt értéket
                    if progress and " / " in progress:
                        try:
                            # Inicializáljuk az eta_samples dict-et, ha még nem létezik
                            if not hasattr(self, 'eta_samples'):
                                self.eta_samples = {}

                            # Ha vannak összegyűjtött ETA minták az utóbbi 5 másodpercből
                            if item_id in self.eta_samples and len(self.eta_samples[item_id]) > 0:
                                # Átlagoljuk az utóbbi 5 másodperc ETA becsléseit
                                eta_timestamps = [eta_ts for _, eta_ts in self.eta_samples[item_id]]
                                average_eta_timestamp = sum(eta_timestamps) / len(eta_timestamps)

                                # Becsült befejezési idő formázása
                                estimated_end_datetime = datetime.fromtimestamp(average_eta_timestamp)
                                estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")

                                # Frissítjük a becsült befejezési időt az átlagolt értékkel (5 másodpercenként)
                                self.estimated_end_dates[item_id] = estimated_end_str
                                self.encoding_queue.put_nowait(("update", item_id, status,
                                                        current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                        current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                        current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                        progress,
                                                        current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                        estimated_end_str))
                        except (tk.TclError, KeyError, AttributeError, IndexError, queue.Full, ValueError, TypeError) as e:
                            # Hiba esetén naplózzuk, hogy lássuk mi a probléma
                            import traceback
                            print(f"TIMER HIBA: {e}")
                            traceback.print_exc()
                except (tk.TclError, KeyError, AttributeError, IndexError):
                    # Ha az item_id már nem létezik, kihagyjuk
                    continue
            
            # Törlés a befejezett/sikertelen tételekből
            for item_id in items_to_remove:
                self.clear_encoding_times(item_id)

            # DEFENSIVE CLEANUP: Régi, elakadt bejegyzések törlése (1 óránál régebbiek)
            # Ez megelőzi a memory leak-et, ha egy worker crash-el/fail-el anélkül,
            # hogy explicit clear_encoding_times() hívást végzett volna
            stale_timeout = 3600  # 1 óra (másodpercben)
            stale_items = []
            for item_id, start_time in list(self.encoding_start_times.items()):
                if current_time - start_time > stale_timeout:
                    stale_items.append(item_id)
            
            for item_id in stale_items:
                self.clear_encoding_times(item_id)
                # Logoljuk, hogy valami elromlott (ez nem normális működés)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[WARN] ETA cleanup: Régi elakadt bejegyzés törölve (>1h): {item_id}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass

            # Timer újraindítása 5 másodperc múlva (5 másodperces mintavételi ablak)
            if hasattr(self, 'root') and self.root.winfo_exists():
                self.estimated_end_timer = self.root.after(5000, update_estimated_end_times)

        if hasattr(self, 'estimated_end_timer') and self.estimated_end_timer is not None:
            try:
                self.root.after_cancel(self.estimated_end_timer)
            except Exception:
                pass
            self.estimated_end_timer = None

        # Első indítás 5 másodperc múlva (5 másodperces mintavételi ablak)
        if hasattr(self, 'root') and self.root.winfo_exists():
            self.estimated_end_timer = self.root.after(5000, update_estimated_end_times)

    def log_status(self, message):
        if LOG_WRITER:
            try:
                LOG_WRITER.write(message + "\n")
                LOG_WRITER.flush()
            except Exception:
                pass

    def on_window_resize(self, event=None):
        """Reszponzív layout: mezők és csúszkák méretének beállítása az ablak szélessége alapján"""
        # Csak az ablak resize eseményére reagálunk
        if event is None or event.widget != self.root:
            return
        
        if not hasattr(self, 'source_entry') or not hasattr(self, 'vmaf_slider'):
            return  # Még nincs inicializálva a UI
        
        try:
            # Ablak szélessége
            window_width = self.root.winfo_width()
            
            # Ha még nincs inicializálva az ablak, várunk
            if window_width < 100:
                return
            
            # Alapértelmezett ablak szélesség (1400px)
            default_width = 1400
            
            # Számítási arány (minimum 0.5, maximum 1.5)
            ratio = max(0.5, min(1.5, window_width / default_width))
            
            # Entry mezők új szélessége
            new_entry_width_source = max(
                self.min_entry_width_source,
                int(self.default_entry_width_source * ratio)
            )
            new_entry_width_path = max(
                self.min_entry_width_path,
                int(self.default_entry_width_path * ratio)
            )
            
            # Csúszkák új hossza
            new_slider_length = max(
                self.min_slider_length,
                int(self.default_slider_length * ratio)
            )
            
            # Entry mezők frissítése
            if hasattr(self, 'source_entry'):
                self.source_entry.config(width=new_entry_width_source)
            if hasattr(self, 'dest_entry'):
                self.dest_entry.config(width=new_entry_width_source)
            if hasattr(self, 'ffmpeg_entry'):
                self.ffmpeg_entry.config(width=new_entry_width_path)
            if hasattr(self, 'vdub_entry'):
                self.vdub_entry.config(width=new_entry_width_path)
            if hasattr(self, 'abav1_entry'):
                self.abav1_entry.config(width=new_entry_width_path)
            
            # Csúszkák frissítése
            if hasattr(self, 'vmaf_slider'):
                self.vmaf_slider.config(length=new_slider_length)
            if hasattr(self, 'vmaf_step_slider'):
                self.vmaf_step_slider.config(length=new_slider_length)
            if hasattr(self, 'max_encoded_slider'):
                self.max_encoded_slider.config(length=new_slider_length)
            if hasattr(self, 'resize_slider'):
                self.resize_slider.config(length=new_slider_length)
        except Exception:
            # Hiba esetén ne akadjon el
            pass

    # NOTE: update_max_encoded_label removed - defined in VmafWorkerMixin (takes precedence in MRO)
    # NOTE: update_resize_label removed - defined in VmafWorkerMixin (takes precedence in MRO)

    def _on_max_encoded_mode_change(self, event=None):
        """Handle max_encoded_mode combobox selection change."""
        selected_text = self.max_encoded_mode_combo.get()
        if hasattr(self, '_max_encoded_mode_map') and selected_text in self._max_encoded_mode_map:
            internal_value = self._max_encoded_mode_map[selected_text]
            self.max_encoded_mode.set(internal_value)
        self._save_settings_debounced()
