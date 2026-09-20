/* SPDX-License-Identifier: Apache-2.0
 * Veeam-process adapter for FastDup's native, metadata-only range clone.
 * This is a scoped XFS capability view, not an XFS filesystem implementation.
 * No data-copy fallback, request splitting, rounding or syscall-table patching.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <linux/fs.h>
#include <linux/magic.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <syslog.h>
#include <unistd.h>

/* Packaging supports x86_64 EL10 only; do not silently reuse another ABI. */
_Static_assert(sizeof(void *) == 8, "64-bit ABI required");
_Static_assert(sizeof(struct statfs) == sizeof(struct statfs64), "statfs ABI");
_Static_assert(sizeof(struct statvfs) == sizeof(struct statvfs64), "statvfs ABI");
_Static_assert(sizeof(struct file_clone_range) == 32, "clone ioctl ABI");

/* Standard immutable setters are capability-gated in the VFS before FUSE can
 * authorize a user-namespaced service. These private commands reach FastDup's
 * inode-scoped authority check; they confer no privilege on another UID/path. */
#define FASTDUP_IOC_SETFLAGS _IOW(0xfd, 2, uint32_t)
#define FASTDUP_IOC_FSSETXATTR _IOW(0xfd, 0x20, struct fsxattr)
_Static_assert(FASTDUP_IOC_SETFLAGS == 0x4004fd02UL, "private flags ioctl ABI");
_Static_assert(FASTDUP_IOC_FSSETXATTR == 0x401cfd20UL, "private fsxattr ioctl ABI");

static pthread_once_t mount_once = PTHREAD_ONCE_INIT;
static int root_fd = -1;
static dev_t root_device;
static _Atomic uint64_t clone_calls, clone_bytes, clone_errors;

static void identify_mount(void)
{
    int fd = open("/repository", O_PATH | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    struct stat st;
    struct statfs fs;
    if (fd < 0) return;
    if (fstat(fd, &st) || syscall(SYS_fstatfs, fd, &fs) || fs.f_type != FUSE_SUPER_MAGIC)
        goto reject;
    char path[64], line[4096];
    unsigned int mount_id = 0, candidate;
    snprintf(path, sizeof(path), "/proc/self/fdinfo/%d", fd);
    FILE *info = fopen(path, "re");
    if (!info) goto reject;
    while (fgets(line, sizeof(line), info))
        if (sscanf(line, "mnt_id: %u", &mount_id) == 1) break;
    fclose(info);
    if (!mount_id) goto reject;
    info = fopen("/proc/self/mountinfo", "re");
    if (!info) goto reject;
    int found = 0;
    while (fgets(line, sizeof(line), info)) {
        if (sscanf(line, "%u", &candidate) == 1 && candidate == mount_id
            && strstr(line, " - fuse fastdup ")) {
            found = 1;
            break;
        }
    }
    fclose(info);
    if (!found) goto reject;
    root_fd = fd;
    root_device = st.st_dev;
    return;
reject:
    close(fd);
}

static int managed_fd(int fd)
{
    pthread_once(&mount_once, identify_mount);
    struct stat st;
    return root_fd >= 0 && fstat(fd, &st) == 0 && st.st_dev == root_device;
}

static int managed_path(const char *path)
{
    int fd = open(path, O_PATH | O_CLOEXEC);
    if (fd < 0) return 0;
    int result = managed_fd(fd);
    close(fd);
    return result;
}

/* Used by the companion geometry query; succeeds only for the verified mount. */
int fastdup_reflink_available(void)
{
    pthread_once(&mount_once, identify_mount);
    return root_fd >= 0 && managed_fd(root_fd);
}

static int clone_failure(int error, uint64_t length)
{
    uint64_t count = atomic_fetch_add_explicit(&clone_errors, 1, memory_order_relaxed) + 1;
    if (count <= 8 || (count & (count - 1)) == 0)
        syslog(LOG_WARNING, "fastdup-reflink failure count=%" PRIu64 " bytes=%" PRIu64 " errno=%d", count, length, error);
    errno = error;
    return -1;
}

static int clone_range(int target, struct file_clone_range *range)
{
    struct stat source_st, target_st;
    if (range->src_fd < 0 || range->src_fd > INT_MAX)
        return clone_failure(EBADF, range->src_length);
    int source = (int)range->src_fd;
    if (fstat(source, &source_st) || fstat(target, &target_st))
        return clone_failure(errno, range->src_length);
    if (!managed_fd(source)) return clone_failure(EXDEV, range->src_length);
    if (!S_ISREG(source_st.st_mode) || !S_ISREG(target_st.st_mode))
        return clone_failure(EINVAL, range->src_length);
    int source_flags = fcntl(source, F_GETFL), target_flags = fcntl(target, F_GETFL);
    if (source_flags < 0 || target_flags < 0 || (source_flags & O_ACCMODE) == O_WRONLY
        || (target_flags & O_ACCMODE) == O_RDONLY || (target_flags & O_APPEND))
        return clone_failure(EBADF, range->src_length);
    uint64_t size = (uint64_t)source_st.st_size;
    if (range->src_offset > size) return clone_failure(EINVAL, range->src_length);
    uint64_t length = range->src_length ? range->src_length : size - range->src_offset;
    if (length > size - range->src_offset || range->dest_offset > INT64_MAX
        || length > (uint64_t)INT64_MAX - range->dest_offset)
        return clone_failure(EINVAL, length);
    /* The kernel limits a single copy_file_range to MAX_RW_COUNT. Reject
     * larger requests before any target mutation instead of splitting them. */
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0 || length > ((uint64_t)INT_MAX & ~((uint64_t)page_size - 1)))
        return clone_failure(EOPNOTSUPP, length);
    if (source_st.st_ino == target_st.st_ino && range->src_offset < range->dest_offset + length
        && range->dest_offset < range->src_offset + length)
        return clone_failure(EINVAL, length);
    if (!length) return 0;
    off_t source_offset = (off_t)range->src_offset, target_offset = (off_t)range->dest_offset;
    /* Raw syscall avoids any libc emulation. This exact mount implements one
     * atomic CloneRange and never copies payload or returns a short success. */
    ssize_t result = syscall(SYS_copy_file_range, source, &source_offset,
                             target, &target_offset, (size_t)length, 0);
    if (result < 0) return clone_failure(errno, length);
    if ((uint64_t)result != length) return clone_failure(EIO, length);
    uint64_t count = atomic_fetch_add_explicit(&clone_calls, 1, memory_order_relaxed) + 1;
    uint64_t bytes = atomic_fetch_add_explicit(&clone_bytes, length, memory_order_relaxed) + length;
    if (count == 1 || count % 4096 == 0)
        syslog(LOG_INFO, "fastdup-reflink native_clones=%" PRIu64 " bytes=%" PRIu64, count, bytes);
    return 0;
}

int ioctl(int fd, unsigned long request, ...)
{
    va_list ap;
    va_start(ap, request);
    uintptr_t argument = 0;
    if (request == FICLONE) argument = (uintptr_t)va_arg(ap, int);
    else if (request != FIOCLEX && request != FIONCLEX) argument = (uintptr_t)va_arg(ap, void *);
    va_end(ap);
    if (!managed_fd(fd))
        return (int)syscall(SYS_ioctl, fd, request, argument);
    if (request == FS_IOC_SETFLAGS)
        return (int)syscall(SYS_ioctl, fd, FASTDUP_IOC_SETFLAGS, argument);
    if (request == FS_IOC_FSSETXATTR)
        return (int)syscall(SYS_ioctl, fd, FASTDUP_IOC_FSSETXATTR, argument);
    if (request != FICLONE && request != FICLONERANGE)
        return (int)syscall(SYS_ioctl, fd, request, argument);
    struct file_clone_range range = {0};
    if (request == FICLONE) range.src_fd = (int)argument;
    else {
        /* Preserve ioctl's EFAULT contract; never dereference a caller's
         * untrusted pointer in the interposed process. */
        struct iovec local = {&range, sizeof(range)}, remote = {(void *)argument, sizeof(range)};
        if (syscall(SYS_process_vm_readv, getpid(), &local, 1, &remote, 1, 0) != sizeof(range))
            return clone_failure(EFAULT, 0);
    }
    return clone_range(fd, &range);
}

int fstatfs(int fd, struct statfs *buf)
{
    int result = (int)syscall(SYS_fstatfs, fd, buf);
    if (!result && buf->f_type == FUSE_SUPER_MAGIC && managed_fd(fd)) buf->f_type = XFS_SUPER_MAGIC;
    return result;
}
int statfs(const char *path, struct statfs *buf)
{
    int result = (int)syscall(SYS_statfs, path, buf);
    if (!result && buf->f_type == FUSE_SUPER_MAGIC && managed_path(path)) buf->f_type = XFS_SUPER_MAGIC;
    return result;
}
int fstatfs64(int fd, struct statfs64 *buf) { return fstatfs(fd, (struct statfs *)buf); }
int statfs64(const char *path, struct statfs64 *buf) { return statfs(path, (struct statfs *)buf); }

static pthread_once_t vfs_once = PTHREAD_ONCE_INIT;
static int (*next_statvfs)(const char *, struct statvfs *);
static int (*next_fstatvfs)(int, struct statvfs *);
static void resolve_vfs(void)
{
    next_statvfs = dlsym(RTLD_NEXT, "statvfs");
    next_fstatvfs = dlsym(RTLD_NEXT, "fstatvfs");
}
int statvfs(const char *path, struct statvfs *buf)
{
    pthread_once(&vfs_once, resolve_vfs);
    if (!next_statvfs) { errno = ENOSYS; return -1; }
    int result = next_statvfs(path, buf);
    if (!result && buf->f_type == FUSE_SUPER_MAGIC && managed_path(path)) buf->f_type = XFS_SUPER_MAGIC;
    return result;
}
int fstatvfs(int fd, struct statvfs *buf)
{
    pthread_once(&vfs_once, resolve_vfs);
    if (!next_fstatvfs) { errno = ENOSYS; return -1; }
    int result = next_fstatvfs(fd, buf);
    if (!result && buf->f_type == FUSE_SUPER_MAGIC && managed_fd(fd)) buf->f_type = XFS_SUPER_MAGIC;
    return result;
}
int statvfs64(const char *path, struct statvfs64 *buf) { return statvfs(path, (struct statvfs *)buf); }
int fstatvfs64(int fd, struct statvfs64 *buf) { return fstatvfs(fd, (struct statvfs *)buf); }

__attribute__((destructor)) static void report_clones(void)
{
    uint64_t count = atomic_load_explicit(&clone_calls, memory_order_relaxed);
    uint64_t errors = atomic_load_explicit(&clone_errors, memory_order_relaxed);
    if (count || errors)
        syslog(LOG_INFO, "fastdup-reflink final native_clones=%" PRIu64 " bytes=%" PRIu64 " errors=%" PRIu64,
               count, atomic_load_explicit(&clone_bytes, memory_order_relaxed), errors);
    if (root_fd >= 0) close(root_fd);
}
