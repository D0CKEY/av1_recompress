from .gui_imports import *
from .gui_shared import *

class VmafWorkerMixin:
    # ============================================================================
    # DEPRECATED: vmaf_worker() and ensure_vmaf_worker_running() are NO LONGER USED
    # VMAF/PSNR calculations are now integrated into SVT workers via _process_vmaf_task()
    # See gui_svt_worker.py Line 2106+ for the new implementation
    # This old code is kept for reference only and will be removed in future versions
    # ============================================================================
    
    # The following methods are DEPRECATED and should NOT be called:
    # - vmaf_worker() - Old VMAF worker that reads from VMAF_QUEUE
    # - ensure_vmaf_worker_running() - Old worker starter
    #
    # New architecture:
    # - VMAF tasks go to SVT_QUEUE with 'type': 'vmaf'
    # - SVT workers route them to _process_vmaf_task() in gui_svt_worker.py
    # - No separate VMAF worker threads needed
    
    def vmaf_worker(self, worker_index=0):
        """**DEPRECATED** - DO NOT USE
        
        This method is obsolete. VMAF/PSNR calculations are now handled
        by SVT workers via SVT_QUEUE (see gui_svt_worker.py).
        
        Raises:
            RuntimeError: Always - this method should never be called
        """
        raise RuntimeError(
            "vmaf_worker() is DEPRECATED and should not be called. "
            "VMAF tasks are now processed by SVT workers via SVT_QUEUE. "
            "See gui_svt_worker.py:_process_vmaf_task() for the new implementation."
        )
