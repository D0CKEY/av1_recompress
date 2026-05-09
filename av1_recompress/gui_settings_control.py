from .gui_imports import *
from .gui_shared import *

class SettingsControlMixin:
    """Centralized settings control with confirmation dialogs during encoding.
    
    This mixin consolidates all label update methods that control encoding settings
    to provide a consistent interface and avoid duplication across multiple mixins.
    
    Methods in this mixin should be placed AFTER worker mixins in the MRO to ensure
    they take precedence when resolving method names.
    """
    
    def _confirm_setting_change_during_encoding(self, setting_var, new_value, old_value):
        """Show confirmation dialog when changing settings during encoding.
        
        Args:
            setting_var: The tkinter variable being changed
            new_value: The new value being set
            old_value: The current/old value
            
        Returns:
            bool: True if user accepted the change, False if cancelled
        """
        if not getattr(self, 'is_encoding', False):
            return True
        
        result = messagebox.askyesno(
            t('msg_warning'),
            t('msg_setting_change_while_encoding').format(old=old_value, new=new_value)
        )
        return result
    
    def update_max_encoded_label(self, value):
        """Update max encoded size percentage label with confirmation dialog.
        
        Args:
            value: New percentage value (0-100)
            
        Side Effects:
            - Updates max_encoded_percent variable
            - Updates max_encoded_value_label text
            - May revert to old value if user cancels during encoding
        """
        int_value = int(round(float(value)))
        old_value = getattr(self, '_last_max_encoded_value', int_value)
        
        if not hasattr(self, '_last_max_encoded_value'):
            self._last_max_encoded_value = int_value
        
        if int_value != old_value:
            if not self._confirm_setting_change_during_encoding(
                self.max_encoded_percent, int_value, old_value
            ):
                self.max_encoded_value_label.config(text=f"{old_value}%")
                return
        
        self._last_max_encoded_value = int_value
        self.max_encoded_percent.set(int_value)
        self.max_encoded_value_label.config(text=f"{int_value}%")
    
    def update_resize_label(self, value):
        """Update resize height label with rate limiting during encoding.
        
        Args:
            value: New height value in pixels
            
        Side Effects:
            - Updates resize_height variable (rounded to nearest 10)
            - Updates resize_value_label text
            - Shows warning if changed during encoding (rate limited to once per minute)
        """
        int_value = int(round(float(value) / 10) * 10)  # Round to nearest 10
        old_value = getattr(self, '_last_resize_value', int_value)
        
        if not hasattr(self, '_last_resize_value'):
            self._last_resize_value = int_value
        
        # Azonnal frissítjük a labelt (ne várjon a felhasználó)
        self.resize_value_label.config(text=f"{int_value}p")
        self.resize_height.set(int_value)
        
        # Ha kódolás fut és az érték változott
        if int_value != old_value and getattr(self, 'is_encoding', False):
            # DEBOUNCE: Töröljük az előző időzítőt, ha volt
            if hasattr(self, '_resize_warning_timer') and self._resize_warning_timer is not None:
                try:
                    self.root.after_cancel(self._resize_warning_timer)
                except Exception:
                    pass
            
            # RATE LIMITING: Ellenőrizzük az utolsó figyelmeztetés időpontját
            current_time = time.time()
            last_warning_time = getattr(self, '_last_resize_warning_time', 0)
            time_since_last_warning = current_time - last_warning_time
            
            # Ha még nem telt el 1 perc az utolsó figyelmeztetés óta, ne mutassunk újat
            if time_since_last_warning < 60:
                self._last_resize_value = int_value
                return
            
            # Új időzítő indítása - 3 másodperc múlva jelenítjük meg a figyelmeztetést
            def show_resize_warning():
                messagebox.showwarning(
                    t('msg_warning'),
                    t('msg_resize_while_encoding')
                )
                self._last_resize_warning_time = time.time()
                self._resize_warning_timer = None
            
            self._resize_warning_timer = self.root.after(3000, show_resize_warning)
        
        self._last_resize_value = int_value
    
    def update_svt_preset_label(self, value):
        """Update SVT-AV1 preset label and save settings.
        
        Args:
            value: Preset value (0-13)
            
        Side Effects:
            - Updates svt_preset variable
            - Updates svt_preset_value_label text
            - Triggers debounced settings save
        """
        int_value = int(float(value))
        self.svt_preset.set(int_value)
        self.svt_preset_value_label.config(text=str(int_value))
        self._save_settings_debounced()  # Automatikus mentés debounce-szal
    
    def update_nvenc_workers_label(self, value):
        """Update NVENC worker count label and trigger dynamic scaling.

        Delayed worker start is scheduled only on real user increase
        and only when there is pending NVENC work.
        """
        try:
            workers = int(round(float(value)))
        except (ValueError, TypeError):
            workers = int(self.nvenc_worker_count.get())

        max_workers = self.max_nvenc_consoles if hasattr(self, 'max_nvenc_consoles') else 3
        workers = max(1, min(max_workers, workers))

        self.nvenc_worker_count.set(workers)
        self.current_nvenc_worker_count = workers
        if hasattr(self, 'nvenc_workers_value_label'):
            self.nvenc_workers_value_label.config(text=str(workers))

        self.refresh_nvenc_console_tabs(workers)
        self._save_settings_debounced()

        prev_workers = getattr(self, '_last_nvenc_workers_value', None)
        self._last_nvenc_workers_value = workers
        if prev_workers is None or workers <= prev_workers:
            return

        has_pending_nvenc = len(getattr(self, 'pending_nvenc_tasks', [])) > 0
        if not has_pending_nvenc:
            return

        if hasattr(self, '_delayed_nvenc_start_timer') and self._delayed_nvenc_start_timer is not None:
            try:
                self.root.after_cancel(self._delayed_nvenc_start_timer)
            except Exception:
                pass
            self._delayed_nvenc_start_timer = None

        if not hasattr(self, 'nvenc_worker_threads'):
            self.nvenc_worker_threads = []
        alive_workers = sum(1 for thread in self.nvenc_worker_threads if thread and thread.is_alive())

        if workers > alive_workers:
            msg = f"NVENC worker increase detected ({alive_workers} -> {workers}), starting in 10s..."
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"{msg}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            if hasattr(self, 'status_label'):
                self.status_label.config(text=msg)
            self._delayed_nvenc_start_timer = self.root.after(
                10000, lambda: self._start_delayed_nvenc_workers(workers)
            )

    def update_svt_workers_label(self, value):
        """Update SVT worker count label and trigger dynamic scaling.

        Delayed worker start is scheduled only on real user increase
        and only when there is pending SVT work.
        """
        try:
            workers = int(round(float(value)))
        except (ValueError, TypeError):
            workers = int(self.svt_worker_count.get())

        workers = max(1, min(3, workers))
        self.svt_worker_count.set(workers)
        self.current_svt_worker_count = workers

        if hasattr(self, 'svt_workers_value_label'):
            self.svt_workers_value_label.config(text=str(workers))

        self.refresh_svt_console_tabs(workers)
        self._save_settings_debounced()

        prev_workers = getattr(self, '_last_svt_workers_value', None)
        self._last_svt_workers_value = workers
        if prev_workers is None or workers <= prev_workers:
            return

        has_pending_svt = len(getattr(self, 'pending_svt_tasks', [])) > 0
        if not has_pending_svt:
            return

        if hasattr(self, '_delayed_svt_start_timer') and self._delayed_svt_start_timer is not None:
            try:
                self.root.after_cancel(self._delayed_svt_start_timer)
            except Exception:
                pass
            self._delayed_svt_start_timer = None

        if not hasattr(self, 'svt_worker_threads'):
            self.svt_worker_threads = []
        alive_workers = sum(1 for thread in self.svt_worker_threads if thread and thread.is_alive())

        if workers > alive_workers:
            msg = f"SVT worker increase detected ({alive_workers} -> {workers}), starting in 10s..."
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"{msg}\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            if hasattr(self, 'status_label'):
                self.status_label.config(text=msg)
            self._delayed_svt_start_timer = self.root.after(
                10000, lambda: self._start_delayed_svt_workers(workers)
            )
