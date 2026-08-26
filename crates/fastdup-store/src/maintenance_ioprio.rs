#![allow(unsafe_code)]

//! Minimal Linux adapter for process-local maintenance I/O scheduling.
//!
//! The unsafe syscall is confined here. Callers expose only the safe operation
//! of placing the current thread in the work-conserving idle I/O class.

use std::io;

const IOPRIO_WHO_PROCESS: libc::c_int = 1;
const IOPRIO_CLASS_SHIFT: u32 = 13;
const IOPRIO_CLASS_IDLE: u32 = 3;

/// Moves only the calling Linux thread into the idle I/O scheduling class.
///
/// Idle-class requests run when the block scheduler has no higher-class work.
/// A short-lived maintenance worker is used because an unprivileged thread
/// cannot reliably promote itself again after entering this class.
pub(crate) fn set_current_thread_idle() -> io::Result<()> {
    let priority = IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT;
    // SAFETY: `SYS_ioprio_set` receives only integer values. WHO_PROCESS with
    // `who == 0` addresses the calling thread, and `priority` is one valid
    // class-only Linux ioprio value. No pointer or borrowed memory crosses the
    // syscall. A negative result is converted from the thread-local errno.
    let result = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, priority) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
