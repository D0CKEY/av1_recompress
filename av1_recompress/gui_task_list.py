from .gui_imports import *
from .gui_shared import *


class TaskListMixin:
    """
    Task list based queue management (replaces queue.Queue).
    
    This mixin provides thread-safe task list operations for SVT-AV1 and NVENC encoding.
    Tasks are stored in ordered lists and sorted by tree position (GUI order).
    
    Key features:
    - Thread-safe operations using task_list_lock
    - Duplicate prevention (same video cannot be added twice)
    - ALL tasks sorted by tree position (no manual priority)
    - Tree order determines execution order (top to bottom)
    - Processing tracking via svt_processing_videos / nvenc_processing_videos sets
    """

    def _resolve_hard_rotate_degrees_for_task(self, video_path, item_id, cached_values, explicit_value=None):
        """Resolve hard-rotate degrees for queued task (0/90/180/270)."""
        if explicit_value is not None:
            return normalize_hard_rotate_degrees(explicit_value)

        rotate_val = 0

        try:
            if hasattr(self, 'video_hard_rotate_degrees') and self.video_hard_rotate_degrees is not None:
                if hasattr(self, 'video_hard_rotate_lock') and self.video_hard_rotate_lock:
                    with self.video_hard_rotate_lock:
                        rotate_val = self.video_hard_rotate_degrees.get(video_path, 0)
                else:
                    rotate_val = self.video_hard_rotate_degrees.get(video_path, 0)
        except Exception:
            rotate_val = 0

        if rotate_val in (90, 180, 270):
            return int(rotate_val)

        rotate_idx = self.COLUMN_INDEX.get('hard_rotate', -1) if hasattr(self, 'COLUMN_INDEX') else -1
        if rotate_idx >= 0 and cached_values and len(cached_values) > rotate_idx:
            rotate_val = normalize_hard_rotate_degrees(cached_values[rotate_idx])
            if rotate_val in (90, 180, 270):
                return rotate_val

        try:
            rotate_val = self.get_tree_item_meta(item_id, 'hard_rotate_degrees', 0)
        except Exception:
            rotate_val = 0
        return normalize_hard_rotate_degrees(rotate_val)
    
    def add_to_svt_queue(self, video_path, item_id, task_type='encode', is_manual=False,
                         pre_cached_values=None, pre_cached_tags=None,
                         pre_cached_tree_index=None, queue_front=False, **kwargs):
        """
        Add a manual or auto encode/VMAF task to the SVT queue.

        Args:
            video_path: Path to the video file
            item_id: Tree item ID
            task_type: 'encode' or 'vmaf'
            is_manual: True = manual action (for tracking), False = auto task. Does NOT affect execution order!
            pre_cached_values: Pre-read tree values (for thread-safe calls from non-GUI threads)
            pre_cached_tags: Pre-read tree tags (for thread-safe calls from non-GUI threads)
            pre_cached_tree_index: Optional tree index snapshot from GUI thread.
            queue_front: If True, this task is prioritized ahead of normal queued tasks.
            **kwargs: Additional parameters (encoder_type, cq, quality_check, etc.)

        Returns:
            bool: True if task was added, False if already queued/processing
        """
        # THREAD-SAFETY FIX: Use pre-cached data if provided (from background thread),
        # otherwise cache tree state NOW (on GUI thread) for safe worker access
        if pre_cached_values is not None:
            cached_values = list(pre_cached_values)
            cached_tags = tuple(pre_cached_tags) if pre_cached_tags else ()
        else:
            cached_values: list = []
            cached_tags: tuple = ()
            if hasattr(self, 'tree') and self.tree:
                try:
                    cached_values = list(self.tree.item(item_id, 'values'))
                    cached_tags = self.tree.item(item_id, 'tags')
                except (tk.TclError, KeyError, AttributeError):
                    pass

        explicit_hard_rotate = kwargs.pop('hard_rotate_degrees', None)
        hard_rotate_degrees = self._resolve_hard_rotate_degrees_for_task(
            video_path=video_path,
            item_id=item_id,
            cached_values=cached_values,
            explicit_value=explicit_hard_rotate,
        )

        # THREAD-SAFETY: vdub_validation_disabled may be passed via kwargs (pre-cached
        # on GUI thread) to avoid tkinter .get() from background threads.
        vdub_disabled = kwargs.pop('vdub_validation_disabled', None)
        if vdub_disabled is None:
            vdub_disabled = self.vdub_validation_disabled.get() if hasattr(self, 'vdub_validation_disabled') else False

        deband_enabled = kwargs.pop('deband_enabled', None)
        if deband_enabled is None:
            deband_enabled = getattr(self, 'current_deband_enabled', True)

        force_8bit_denoised_master = kwargs.pop('force_8bit_denoised_master', None)
        if force_8bit_denoised_master is None:
            force_8bit_denoised_master = getattr(self, 'current_force_8bit_denoised_master', False)

        # THREAD-SAFETY: Pre-cache svt_preset and crf_increment on GUI thread.
        # Worker threads MUST NOT call self.svt_preset.get() / self.crf_increment.get()
        # because tkinter variable access from non-GUI threads can deadlock the Tcl interpreter.
        svt_preset_cached = kwargs.pop('svt_preset', None)
        if svt_preset_cached is None:
            svt_preset_cached = self.svt_preset.get() if hasattr(self, 'svt_preset') else 2

        crf_increment_cached = kwargs.pop('crf_increment', None)
        if crf_increment_cached is None:
            crf_increment_cached = self.crf_increment.get() if hasattr(self, 'crf_increment') else 1

        max_encoded_mode_cached = kwargs.pop('max_encoded_mode', None)
        if max_encoded_mode_cached is None:
            max_encoded_mode_cached = self.max_encoded_mode.get() if hasattr(self, 'max_encoded_mode') else 'full'

        task = {
            'video_path': video_path,
            'item_id': item_id,
            'type': task_type,
            'is_manual': is_manual,
            'cached_values': cached_values,  # Thread-safe snapshot of tree values
            'cached_tags': cached_tags,       # Thread-safe snapshot of tree tags
            'tree_order_index': pre_cached_tree_index,
            'queue_front': bool(queue_front),
            'hard_rotate_degrees': hard_rotate_degrees,
            'vdub_validation_disabled': vdub_disabled,
            'deband_enabled': bool(deband_enabled),
            'force_8bit_denoised_master': bool(force_8bit_denoised_master),
            'svt_preset': int(svt_preset_cached),
            'crf_increment': int(crf_increment_cached),
            'max_encoded_mode': str(max_encoded_mode_cached),
            **kwargs
        }

        with self.task_list_lock:
            # CRITICAL FIX: VMAF tasks can be added even if video is currently being encoded
            # VMAF runs on the OUTPUT file, not the source, so there's no actual conflict
            # Only check processing set for ENCODE tasks
            if task_type == 'encode':
                # Duplicate check: if already in processing set
                if video_path in self.svt_processing_videos:
                    # Already processing
                    if LOG_WRITER:
                        LOG_WRITER.write(f"  [WARN] SVT encode task for {Path(video_path).name} already processing, skipping\n")
                    return False

            # Check if same task already in the list
            existing_task = any(t['video_path'] == video_path and t.get('type') == task_type for t in self.pending_svt_tasks)
            if existing_task:
                # Already scheduled
                if LOG_WRITER:
                    LOG_WRITER.write(f"  [WARN] SVT {task_type} task for {Path(video_path).name} already scheduled, skipping\n")
                return False

            # ALL tasks are appended to the end - sorting by tree position happens in get_next_svt_task()
            # No manual priority - tree order always wins!
            self.pending_svt_tasks.append(task)

            manual_str = " (MANUAL)" if is_manual else ""
            front_str = " (FRONT)" if queue_front else ""
            task_type_str = f" ({task_type})" if task_type != 'encode' else ""
            if LOG_WRITER:
                LOG_WRITER.write(f"  [OK] Added SVT{task_type_str} task for {Path(video_path).name}{manual_str}{front_str}\n")

            # NOTE: Processing set is updated when worker actually picks up the task (in get_next_svt_task)

        return True
    
    def add_to_nvenc_queue(self, video_path, item_id, is_manual=False,
                          pre_cached_values=None, pre_cached_tags=None,
                          pre_cached_tree_index=None, queue_front=False, **kwargs):
        """
        Add a manual or auto NVENC encoding task to the NVENC queue.

        Args:
            video_path: Path to the video file
            item_id: Tree item ID
            is_manual: True = manual action (for tracking), False = auto task. Does NOT affect execution order!
            pre_cached_values: Pre-read tree values (for thread-safe calls from non-GUI threads)
            pre_cached_tags: Pre-read tree tags (for thread-safe calls from non-GUI threads)
            pre_cached_tree_index: Optional tree index snapshot from GUI thread.
            queue_front: If True, this task is prioritized ahead of normal queued tasks.
            **kwargs: Additional parameters (target_cq, vmaf_step, etc.)

        Returns:
            bool: True if task was added, False if already queued/processing
        """
        # THREAD-SAFETY FIX: Use pre-cached data if provided (from background thread),
        # otherwise cache tree state NOW (on GUI thread) for safe worker access
        if pre_cached_values is not None:
            cached_values = list(pre_cached_values)
            cached_tags = tuple(pre_cached_tags) if pre_cached_tags else ()
        else:
            cached_values: list = []
            cached_tags: tuple = ()
            if hasattr(self, 'tree') and self.tree:
                try:
                    cached_values = list(self.tree.item(item_id, 'values'))
                    cached_tags = self.tree.item(item_id, 'tags')
                except (tk.TclError, KeyError, AttributeError):
                    pass

        explicit_hard_rotate = kwargs.pop('hard_rotate_degrees', None)
        hard_rotate_degrees = self._resolve_hard_rotate_degrees_for_task(
            video_path=video_path,
            item_id=item_id,
            cached_values=cached_values,
            explicit_value=explicit_hard_rotate,
        )

        # THREAD-SAFETY: vdub_validation_disabled may be passed via kwargs (pre-cached
        # on GUI thread) to avoid tkinter .get() from background threads.
        vdub_disabled = kwargs.pop('vdub_validation_disabled', None)
        if vdub_disabled is None:
            vdub_disabled = self.vdub_validation_disabled.get() if hasattr(self, 'vdub_validation_disabled') else False

        deband_enabled = kwargs.pop('deband_enabled', None)
        if deband_enabled is None:
            deband_enabled = getattr(self, 'current_deband_enabled', True)

        force_8bit_denoised_master = kwargs.pop('force_8bit_denoised_master', None)
        if force_8bit_denoised_master is None:
            force_8bit_denoised_master = getattr(self, 'current_force_8bit_denoised_master', False)

        task = {
            'video_path': video_path,
            'item_id': item_id,
            'type': 'encode',  # NVENC only does encoding (no VMAF)
            'is_manual': is_manual,
            'cached_values': cached_values,  # Thread-safe snapshot of tree values
            'cached_tags': cached_tags,       # Thread-safe snapshot of tree tags
            'tree_order_index': pre_cached_tree_index,
            'queue_front': bool(queue_front),
            'hard_rotate_degrees': hard_rotate_degrees,
            'vdub_validation_disabled': vdub_disabled,
            'deband_enabled': bool(deband_enabled),
            'force_8bit_denoised_master': bool(force_8bit_denoised_master),
            **kwargs
        }
        
        with self.task_list_lock:
            # Duplicate check: if already in processing set
            if video_path in self.nvenc_processing_videos:
                # Already processing
                if LOG_WRITER:
                    LOG_WRITER.write(f"  [WARN] NVENC task for {Path(video_path).name} already processing, skipping\n")
                return False
            
            # Check if same task already in pending or manual NVENC lists
            existing_task = any(t['video_path'] == video_path for t in self.pending_nvenc_tasks)
            if not existing_task and hasattr(self, 'manual_nvenc_tasks'):
                existing_task = any(t['video_path'] == video_path for t in self.manual_nvenc_tasks)
            if existing_task:
                # Already scheduled
                if LOG_WRITER:
                    LOG_WRITER.write(f"  [WARN] NVENC task for {Path(video_path).name} already scheduled, skipping\n")
                return False
            
            # ALL tasks are appended to the end - sorting by tree position happens in get_next_nvenc_task()
            # No manual priority - tree order always wins!
            self.pending_nvenc_tasks.append(task)
            
            manual_str = " (MANUAL)" if is_manual else ""
            front_str = " (FRONT)" if queue_front else ""
            if LOG_WRITER:
                LOG_WRITER.write(f"  [OK] Added NVENC task for {Path(video_path).name}{manual_str}{front_str}\n")

            # NOTE: Processing set is updated when worker actually picks up the task (in get_next_nvenc_task)

        return True
    
    def get_next_svt_task(self):
        """
        Worker calls this when it becomes available.
        
        Returns:
            dict: Task object or None if no pending tasks
        """
        with self.task_list_lock:
            if not self.pending_svt_tasks:
                return None
            
            # Sort by tree position (all tasks - no manual priority)
            self._sort_svt_tasks_by_tree_position()
            
            # Return first element and remove from list
            task = self.pending_svt_tasks.pop(0)
            
            # CRITICAL: Add to processing set NOW (when worker actually picks it up)
            # This prevents race conditions and duplicate processing
            video_path = task.get('video_path')
            if video_path:
                self.svt_processing_videos.add(video_path)
            
            if LOG_WRITER:
                video_name = Path(task['video_path']).name
                task_type = task.get('type', 'encode')
                is_manual = task.get('is_manual', False)
                manual_str = " (MANUAL)" if is_manual else ""
                LOG_WRITER.write(f"  -> Worker picking up SVT {task_type} task: {video_name}{manual_str}\n")
            
            return task
    
    def get_next_svt_vmaf_task(self):
        """
        Get the next VMAF/PSNR task from the pending list.
        Used during graceful stop to finish VMAF calculations before exiting.
        
        Returns:
            dict: VMAF task object or None if no VMAF tasks pending
        """
        with self.task_list_lock:
            for i, task in enumerate(self.pending_svt_tasks):
                if task.get('type') == 'vmaf':
                    task = self.pending_svt_tasks.pop(i)
                    video_path = task.get('video_path')
                    if video_path:
                        self.svt_processing_videos.add(video_path)
                    if LOG_WRITER:
                        video_name = Path(task['video_path']).name
                        LOG_WRITER.write(f"  -> Worker picking up SVT VMAF task (graceful stop): {video_name}\n")
                    return task
        return None
    
    def has_pending_vmaf_tasks(self):
        """Check if there are any VMAF tasks in the pending list."""
        with self.task_list_lock:
            return any(t.get('type') == 'vmaf' for t in self.pending_svt_tasks)
    
    def get_next_nvenc_task(self):
        """
        Worker calls this when it becomes available.
        
        Returns:
            dict: Task object or None if no pending tasks
        """
        with self.task_list_lock:
            if not self.pending_nvenc_tasks:
                return None
            
            # Sort by tree position (all tasks - no manual priority)
            self._sort_nvenc_tasks_by_tree_position()
            
            # Return first element and remove from list
            task = self.pending_nvenc_tasks.pop(0)
            
            # CRITICAL: Add to processing set NOW (when worker actually picks it up)
            # This prevents race conditions and duplicate processing
            video_path = task.get('video_path')
            if video_path:
                self.nvenc_processing_videos.add(video_path)
            
            if LOG_WRITER:
                video_name = Path(task['video_path']).name
                is_manual = task.get('is_manual', False)
                manual_str = " (MANUAL)" if is_manual else ""
                LOG_WRITER.write(f"  -> Worker picking up NVENC task: {video_name}{manual_str}\n")
            
            return task
    
    def _update_cached_tree_order(self):
        """
        Update cached tree children order. MUST be called from GUI thread only.
        Worker threads use _cached_tree_order for sorting instead of calling tree.get_children().
        """
        if hasattr(self, 'tree') and self.tree:
            try:
                self._cached_tree_order = list(self.tree.get_children())
            except (tk.TclError, RuntimeError):
                pass  # Keep old cached order if tree is unavailable

    def _sort_svt_tasks_by_tree_position(self):
        """
        Sort ALL pending SVT tasks by tree position.
        No manual priority - tree order always determines execution order.
        Uses _cached_tree_order (updated by GUI thread) instead of direct tree access.
        """
        if self.pending_svt_tasks:
            tree_children = self._cached_tree_order if self._cached_tree_order else []

            def get_tree_index(task):
                pre_cached_idx = task.get('tree_order_index')
                if isinstance(pre_cached_idx, int) and pre_cached_idx >= 0:
                    return pre_cached_idx
                item_id = task.get('item_id')
                try:
                    return tree_children.index(item_id)
                except ValueError:
                    return len(tree_children)

            self.pending_svt_tasks.sort(
                key=lambda task: (
                    0 if task.get('queue_front') else 1,
                    get_tree_index(task),
                )
            )

    def _sort_nvenc_tasks_by_tree_position(self):
        """
        Sort ALL pending NVENC tasks by tree position.
        No manual priority - tree order always determines execution order.
        Uses _cached_tree_order (updated by GUI thread) instead of direct tree access.
        """
        if self.pending_nvenc_tasks:
            tree_children = self._cached_tree_order if self._cached_tree_order else []

            def get_tree_index(task):
                pre_cached_idx = task.get('tree_order_index')
                if isinstance(pre_cached_idx, int) and pre_cached_idx >= 0:
                    return pre_cached_idx
                item_id = task.get('item_id')
                try:
                    return tree_children.index(item_id)
                except ValueError:
                    return len(tree_children)

            self.pending_nvenc_tasks.sort(
                key=lambda task: (
                    0 if task.get('queue_front') else 1,
                    get_tree_index(task),
                )
            )
    
    def complete_svt_task(self, video_path):
        """
        Called when a task finishes (success or failure).
        Removes the video_path from the processing set.
        
        Args:
            video_path: Path to the video file
        """
        with self.task_list_lock:
            self.svt_processing_videos.discard(video_path)
            if LOG_WRITER:
                LOG_WRITER.write(f"  [OK] SVT task completed for {Path(video_path).name}\n")
    
    def complete_nvenc_task(self, video_path):
        """
        Called when a task finishes (success or failure).
        Removes the video_path from the processing set.
        
        Args:
            video_path: Path to the video file
        """
        with self.task_list_lock:
            self.nvenc_processing_videos.discard(video_path)
            if LOG_WRITER:
                LOG_WRITER.write(f"  [OK] NVENC task completed for {Path(video_path).name}\n")
    
    def clear_all_svt_tasks(self):
        """
        Clear all pending SVT tasks and processing videos.
        Used during graceful stop or shutdown.
        """
        with self.task_list_lock:
            count = len(self.pending_svt_tasks)
            self.pending_svt_tasks.clear()
            self.svt_processing_videos.clear()
            if LOG_WRITER and count > 0:
                LOG_WRITER.write(f"  [DEL] Cleared {count} pending SVT tasks\n")
    
    def clear_all_nvenc_tasks(self):
        """
        Clear all pending NVENC tasks and processing videos.
        Used during graceful stop or shutdown.
        """
        with self.task_list_lock:
            count = len(self.pending_nvenc_tasks)
            self.pending_nvenc_tasks.clear()
            self.nvenc_processing_videos.clear()
            if LOG_WRITER and count > 0:
                LOG_WRITER.write(f"  [DEL] Cleared {count} pending NVENC tasks\n")
    
    def get_pending_task_counts(self):
        """
        Get counts of pending and processing tasks.
        
        Returns:
            dict: {
                'svt_pending': int,
                'svt_processing': int,
                'nvenc_pending': int,
                'nvenc_processing': int
            }
        """
        with self.task_list_lock:
            return {
                'svt_pending': len(self.pending_svt_tasks),
                'svt_processing': len(self.svt_processing_videos),
                'nvenc_pending': len(self.pending_nvenc_tasks),
                'nvenc_processing': len(self.nvenc_processing_videos)
            }
