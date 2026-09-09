/*
 * fastdup Samba VFS adapter
 *
 * Copyright (C) 2026 fastdup contributors
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 */

#include "includes.h"
#include "system/filesys.h"
#include "smbd/smbd.h"
#include "smbd/globals.h"
#include "lib/util/tevent_ntstatus.h"
#include "offload_token.h"
#include "lib/pthreadpool/pthreadpool_tevent.h"
#include "vfs_fastdup_contract.h"

#include <inttypes.h>
#include <unistd.h>
#include <sys/xattr.h>

#define FASTDUP_MODULE "fastdup"
#define FASTDUP_DEFAULT_ALIGNMENT FASTDUP_CLONE_ALIGNMENT_V1
#define FASTDUP_DEFAULT_MAX_CLONE ((uint64_t)1073741824)
#define FASTDUP_LINUX_SINGLE_COPY_MAX ((uint64_t)0x7ffff000)
#define FASTDUP_GET_INTEGRITY_BYTES ((uint32_t)16)
#define FASTDUP_INTEGRITY_XATTR "user.fastdup.smb-integrity.v1"

/* Missing from Samba 4.23's smb_constants.h. */
#ifndef FSCTL_GET_INTEGRITY_INFORMATION
#define FSCTL_GET_INTEGRITY_INFORMATION 0x0009027c
#endif

struct fastdup_config {
	bool enabled;
	uint64_t alignment;
	uint64_t maximum_clone_bytes;
};

struct fastdup_fsp_state {
	struct fastdup_handle_fence fence;
};

static struct vfs_offload_ctx *fastdup_offload_ctx;

static bool fastdup_power_of_two(uint64_t value)
{
	return value != 0 && (value & (value - 1)) == 0;
}

static struct fastdup_fsp_state *fastdup_fsp_state(
	struct vfs_handle_struct *handle,
	struct files_struct *fsp)
{
	struct fastdup_fsp_state *state = VFS_FETCH_FSP_EXTENSION(handle, fsp);

	if (state != NULL) {
		return state;
	}
	return VFS_ADD_FSP_EXTENSION(handle, fsp, struct fastdup_fsp_state,
				     NULL);
}

static NTSTATUS fastdup_contract_ntstatus(enum fastdup_contract_status status)
{
	switch (status) {
	case FASTDUP_CONTRACT_OK:
		return NT_STATUS_OK;
	case FASTDUP_CONTRACT_INVALID_PARAMETER:
	case FASTDUP_CONTRACT_TARGET_NOT_PRESIZED:
	case FASTDUP_CONTRACT_MISALIGNED:
	case FASTDUP_CONTRACT_CLONE_TOO_LARGE:
		return NT_STATUS_INVALID_PARAMETER;
	case FASTDUP_CONTRACT_UNSUPPORTED_INTEGRITY_STATE:
		return NT_STATUS_INVALID_DEVICE_REQUEST;
	case FASTDUP_CONTRACT_OVERLAP:
		return NT_STATUS_NOT_SUPPORTED;
	case FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS:
		return NT_STATUS_END_OF_FILE;
	}
	return NT_STATUS_INTERNAL_ERROR;
}

static int fastdup_connect(struct vfs_handle_struct *handle,
			   const char *service,
			   const char *user)
{
	struct fastdup_config *config = NULL;
	unsigned long long configured_alignment;
	unsigned long long configured_maximum;
	int result;

	result = SMB_VFS_NEXT_CONNECT(handle, service, user);
	if (result < 0) {
		return result;
	}

	config = talloc_zero(handle->conn, struct fastdup_config);
	if (config == NULL) {
		errno = ENOMEM;
		return -1;
	}
	config->enabled = lp_parm_bool(SNUM(handle->conn), FASTDUP_MODULE,
				       "enabled", false);
	configured_alignment = lp_parm_ulonglong(
		SNUM(handle->conn), FASTDUP_MODULE, "clone alignment",
		FASTDUP_DEFAULT_ALIGNMENT);
	configured_maximum = lp_parm_ulonglong(
		SNUM(handle->conn), FASTDUP_MODULE, "maximum clone bytes",
		FASTDUP_DEFAULT_MAX_CLONE);
	config->alignment = configured_alignment;
	config->maximum_clone_bytes = configured_maximum;

	if (!fastdup_power_of_two(config->alignment) ||
	    config->alignment < 4096 || config->maximum_clone_bytes == 0 ||
	    config->maximum_clone_bytes > FASTDUP_LINUX_SINGLE_COPY_MAX ||
	    config->maximum_clone_bytes % config->alignment != 0) {
		DBG_ERR("invalid fastdup clone alignment=%" PRIu64
			" maximum=%" PRIu64 "\n",
			config->alignment, config->maximum_clone_bytes);
		errno = EINVAL;
		return -1;
	}

	SMB_VFS_HANDLE_SET_DATA(handle, config, NULL, struct fastdup_config,
				return -1);
	return 0;
}

static uint32_t fastdup_fs_capabilities(
	struct vfs_handle_struct *handle,
	enum timestamp_set_resolution *timestamp_resolution)
{
	struct fastdup_config *config = NULL;
	uint32_t capabilities;

	capabilities = SMB_VFS_NEXT_FS_CAPABILITIES(handle,
						 timestamp_resolution);
	SMB_VFS_HANDLE_GET_DATA(handle, config, struct fastdup_config,
				return capabilities);
	if (config->enabled) {
		capabilities |= FILE_SUPPORTS_BLOCK_REFCOUNTING;
	}
	return capabilities;
}

static NTSTATUS fastdup_get_integrity(struct vfs_handle_struct *handle,
				     struct files_struct *fsp,
				     uint16_t *algorithm)
{
	struct fastdup_config *config = NULL;
	uint8_t stored[2];
	ssize_t length = SMB_VFS_NEXT_FGETXATTR(handle, fsp,
		FASTDUP_INTEGRITY_XATTR, stored, sizeof(stored));
	if (length == -1) {
		if (errno == ENOATTR) {
			*algorithm = FASTDUP_CHECKSUM_NONE;
			return NT_STATUS_OK;
		}
		return errno == ERANGE ? NT_STATUS_DATA_ERROR : map_nt_error_from_unix(errno);
	}
	if (fastdup_integrity_decode_v1(stored, (size_t)length, algorithm) !=
	    FASTDUP_CONTRACT_OK) {
		return NT_STATUS_DATA_ERROR;
	}
	SMB_VFS_HANDLE_GET_DATA(handle, config, struct fastdup_config,
				return NT_STATUS_INTERNAL_ERROR);
	if (fastdup_integrity_effective_v1(*algorithm,
		(uint32_t)config->alignment, algorithm) != FASTDUP_CONTRACT_OK) {
		return NT_STATUS_DATA_ERROR;
	}
	return NT_STATUS_OK;
}

static NTSTATUS fastdup_fsctl(struct vfs_handle_struct *handle,
			      struct files_struct *fsp,
			      TALLOC_CTX *ctx,
			      uint32_t function,
			      uint16_t request_flags,
			      const uint8_t *input,
			      uint32_t input_length,
			      uint8_t **output,
			      uint32_t maximum_output_length,
			      uint32_t *output_length)
{
	struct fastdup_config *config = NULL;
	struct fastdup_fsp_state *state = NULL;
	enum fastdup_contract_status contract_status;
	uint64_t sequence;
	size_t produced = 0;
	uint8_t *reply = NULL;
	uint8_t stored[2];
	uint16_t algorithm;
	NTSTATUS status;
	int result;

	(void)request_flags;
	SMB_VFS_HANDLE_GET_DATA(handle, config, struct fastdup_config,
				return NT_STATUS_INTERNAL_ERROR);
	if (!config->enabled) {
		return SMB_VFS_NEXT_FSCTL(handle, fsp, ctx, function,
					  request_flags, input, input_length,
					  output, maximum_output_length,
					  output_length);
	}

	switch (function) {
	case FSCTL_SET_INTEGRITY_INFORMATION:
		contract_status = fastdup_integrity_set_v1(input, input_length);
		if (contract_status != FASTDUP_CONTRACT_OK) {
			return fastdup_contract_ntstatus(contract_status);
		}
		if (fsp == NULL || fsp_get_pathref_fd(fsp) == -1) {
			return NT_STATUS_INVALID_PARAMETER;
		}
		if (!CAN_WRITE(handle->conn) ||
		    !(fsp->access_mask & (SEC_FILE_WRITE_DATA | SEC_FILE_WRITE_ATTRIBUTE))) {
			return NT_STATUS_ACCESS_DENIED;
		}
		status = fastdup_get_integrity(handle, fsp, &algorithm);
		if (!NT_STATUS_IS_OK(status)) {
			return status;
		}
		contract_status = fastdup_integrity_resolve_v1(input, input_length,
			algorithm, (uint32_t)config->alignment, stored);
		if (contract_status != FASTDUP_CONTRACT_OK) {
			return fastdup_contract_ntstatus(contract_status);
		}
		state = fastdup_fsp_state(handle, fsp);
		if (state == NULL) {
			return NT_STATUS_NO_MEMORY;
		}
		if (!fastdup_handle_accept(&state->fence, &sequence)) {
			return NT_STATUS_TOO_MANY_COMMANDS;
		}
		/* An atomic inode xattr mutation enters the normal checkpoint window.
		 * UNCHANGED must not race by writing an older value back. */
		result = 0;
		if (input[0] != 0xff || input[1] != 0xff) {
			result = SMB_VFS_NEXT_FSETXATTR(handle, fsp,
				FASTDUP_INTEGRITY_XATTR, stored, sizeof(stored), 0);
		}
		status = result == 0 ? NT_STATUS_OK : map_nt_error_from_unix(errno);
		SMB_ASSERT(fastdup_handle_complete(&state->fence, sequence));
		if (!NT_STATUS_IS_OK(status)) {
			return status;
		}
		*output_length = 0;
		return NT_STATUS_OK;

	case FSCTL_GET_INTEGRITY_INFORMATION:
		if (fsp == NULL || maximum_output_length < FASTDUP_GET_INTEGRITY_BYTES) {
			return NT_STATUS_INVALID_PARAMETER;
		}
		status = fastdup_get_integrity(handle, fsp, &algorithm);
		if (!NT_STATUS_IS_OK(status)) {
			return status;
		}
		reply = talloc_zero_array(ctx, uint8_t,
					  FASTDUP_GET_INTEGRITY_BYTES);
		if (reply == NULL) {
			return NT_STATUS_NO_MEMORY;
		}
		contract_status = fastdup_integrity_get_v1(
			reply, FASTDUP_GET_INTEGRITY_BYTES, algorithm,
			(uint32_t)config->alignment, &produced);
		SMB_ASSERT(contract_status == FASTDUP_CONTRACT_OK);
		SMB_ASSERT(produced == FASTDUP_GET_INTEGRITY_BYTES);
		*output = reply;
		*output_length = FASTDUP_GET_INTEGRITY_BYTES;
		return NT_STATUS_OK;

	default:
		return SMB_VFS_NEXT_FSCTL(handle, fsp, ctx, function,
					  request_flags, input, input_length,
					  output, maximum_output_length,
					  output_length);
	}
}

struct fastdup_offload_read_state {
	struct vfs_handle_struct *handle;
	uint32_t flags;
	uint64_t transfer_length;
	DATA_BLOB token;
};

static void fastdup_offload_read_done(struct tevent_req *subrequest)
{
	struct tevent_req *request = tevent_req_callback_data(
		subrequest, struct tevent_req);
	struct fastdup_offload_read_state *state = tevent_req_data(
		request, struct fastdup_offload_read_state);
	NTSTATUS status;

	status = SMB_VFS_NEXT_OFFLOAD_READ_RECV(
		subrequest, state->handle, state, &state->flags,
		&state->transfer_length, &state->token);
	TALLOC_FREE(subrequest);
	if (tevent_req_nterror(request, status)) {
		return;
	}
	tevent_req_done(request);
}

static struct tevent_req *fastdup_offload_read_send(
	TALLOC_CTX *memory_context,
	struct tevent_context *event_context,
	struct vfs_handle_struct *handle,
	struct files_struct *fsp,
	uint32_t fsctl,
	uint32_t ttl,
	off_t offset,
	size_t to_copy)
{
	struct fastdup_config *config = NULL;
	struct fastdup_offload_read_state *state = NULL;
	struct tevent_req *request = NULL;
	struct tevent_req *subrequest = NULL;
	NTSTATUS status;

	request = tevent_req_create(memory_context, &state,
				    struct fastdup_offload_read_state);
	if (request == NULL) {
		return NULL;
	}
	state->handle = handle;
	SMB_VFS_HANDLE_GET_DATA(handle, config, struct fastdup_config,
				tevent_req_nterror(request,
						    NT_STATUS_INTERNAL_ERROR);
				return tevent_req_post(request, event_context));

	if (!config->enabled || fsctl != FSCTL_DUP_EXTENTS_TO_FILE) {
		subrequest = SMB_VFS_NEXT_OFFLOAD_READ_SEND(
			memory_context, event_context, handle, fsp, fsctl, ttl,
			offset, to_copy);
		if (tevent_req_nomem(subrequest, request)) {
			return tevent_req_post(request, event_context);
		}
		tevent_req_set_callback(subrequest, fastdup_offload_read_done,
					request);
		return request;
	}

	status = vfs_offload_token_ctx_init(fsp->conn->sconn->client,
					    &fastdup_offload_ctx);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	status = vfs_offload_token_create_blob(state, fsp, fsctl,
					       &state->token);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	status = vfs_offload_token_db_store_fsp(fastdup_offload_ctx, fsp,
						&state->token);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	tevent_req_done(request);
	return tevent_req_post(request, event_context);
}

static NTSTATUS fastdup_offload_read_recv(struct tevent_req *request,
					  struct vfs_handle_struct *handle,
					  TALLOC_CTX *memory_context,
					  uint32_t *flags,
					  uint64_t *transfer_length,
					  DATA_BLOB *token)
{
	struct fastdup_offload_read_state *state = tevent_req_data(
		request, struct fastdup_offload_read_state);
	NTSTATUS status;

	(void)handle;
	if (tevent_req_is_nterror(request, &status)) {
		tevent_req_received(request);
		return status;
	}
	*flags = state->flags;
	*transfer_length = state->transfer_length;
	token->length = state->token.length;
	token->data = talloc_move(memory_context, &state->token.data);
	tevent_req_received(request);
	return NT_STATUS_OK;
}

/* All scheduler state belongs to the smbd event thread. Workers receive
 * only owned descriptors/offsets, never Samba identities or mutable handles. */
#define FASTDUP_ASYNC_CLONES 8
#define FASTDUP_PENDING_CLONES 64
struct fastdup_offload_write_state {
    struct vfs_handle_struct *handle;
    off_t copied;
    struct tevent_req *request;
    struct tevent_context *event_context;
    struct fastdup_fsp_state *target_state;
    struct file_id source_id, target_id;
    uint64_t sequence;
    int source_fd, target_fd;
    off_t source_offset, target_offset, length;
    ssize_t result;
    int error;
    NTSTATUS validation;
    uint64_t alignment, maximum_clone_bytes;
    bool active, queued;
    struct tevent_req *guard;
    struct fastdup_offload_write_state *prev, *next;
};
static struct fastdup_offload_write_state *fastdup_clone_jobs;
static size_t fastdup_clone_pending, fastdup_clone_active;
static void fastdup_clone_schedule(void);

struct fastdup_clone_guard { int unused; };
static int fastdup_clone_state_busy(struct fastdup_offload_write_state *state)
{
    (void)state;
    return -1;
}
static void fastdup_clone_cleanup(struct tevent_req *request, enum tevent_req_state reason)
{
    struct fastdup_offload_write_state *state = tevent_req_data(request, struct fastdup_offload_write_state);
    if (reason != TEVENT_REQ_RECEIVED || !state->queued) { return; }
    /* A cancelled/disconnected caller may free its request while the syscall
     * is uninterruptible. Detach the job, including its handle guard, until
     * the event-thread completion. Callbacks never reference the old request. */
    state->request = NULL;
    (void)talloc_steal(NULL, state);
}
static bool fastdup_clone_conflict(const struct fastdup_offload_write_state *a,
                                  const struct fastdup_offload_write_state *b)
{
    /* Preserve arrival order for write/write and read/write dependencies,
     * including different handles to the same inode. Independent files overlap. */
    return file_id_equal(&a->target_id, &b->target_id) ||
           file_id_equal(&a->source_id, &b->target_id) ||
           file_id_equal(&a->target_id, &b->source_id);
}
static NTSTATUS fastdup_clone_fd_integrity(int fd, uint64_t alignment, uint16_t *algorithm)
{
    uint8_t stored[2];
    ssize_t length = fgetxattr(fd, FASTDUP_INTEGRITY_XATTR, stored, sizeof(stored));
    if (length < 0) {
        if (errno == ENOATTR) { *algorithm = FASTDUP_CHECKSUM_NONE; return NT_STATUS_OK; }
        return errno == ERANGE ? NT_STATUS_DATA_ERROR : map_nt_error_from_unix(errno);
    }
    if (fastdup_integrity_decode_v1(stored, (size_t)length, algorithm) != FASTDUP_CONTRACT_OK ||
        fastdup_integrity_effective_v1(*algorithm, (uint32_t)alignment, algorithm) != FASTDUP_CONTRACT_OK) {
        return NT_STATUS_DATA_ERROR;
    }
    return NT_STATUS_OK;
}
static void fastdup_clone_do(void *private_data)
{
    struct fastdup_offload_write_state *state = private_data;
    struct stat source_stat, target_stat;
    struct fastdup_clone_request request;
    uint16_t source_integrity, target_integrity;
    off_t source = state->source_offset, target = state->target_offset;
    /* The managed profile uses native fastdup descriptors. Query their current
     * native metadata here, so stat/xattr latency cannot block smbd's event loop
     * and queued requests never rely on stale admission-time EOF snapshots. */
    if (fstat(state->source_fd, &source_stat) < 0 || fstat(state->target_fd, &target_stat) < 0) {
        state->validation = map_nt_error_from_unix(errno); return;
    }
    if (source_stat.st_size < 0 || target_stat.st_size < 0) {
        state->validation = NT_STATUS_IO_DEVICE_ERROR; return;
    }
    state->validation = fastdup_clone_fd_integrity(state->source_fd, state->alignment, &source_integrity);
    if (!NT_STATUS_IS_OK(state->validation)) { return; }
    state->validation = fastdup_clone_fd_integrity(state->target_fd, state->alignment, &target_integrity);
    if (!NT_STATUS_IS_OK(state->validation)) { return; }
    if (source_integrity != target_integrity) {
        state->validation = NT_STATUS_INVALID_PARAMETER; return;
    }
    request = (struct fastdup_clone_request) {
        .source_size = source_stat.st_size, .target_size = target_stat.st_size,
        .source_offset = source, .target_offset = target, .length = state->length,
        .alignment = state->alignment, .maximum_length = state->maximum_clone_bytes,
        .same_file = source_stat.st_dev == target_stat.st_dev && source_stat.st_ino == target_stat.st_ino,
    };
    state->validation = fastdup_contract_ntstatus(fastdup_validate_clone_v1(&request));
    if (!NT_STATUS_IS_OK(state->validation)) { return; }
    state->result = copy_file_range(state->source_fd, &source,
        state->target_fd, &target, (size_t)state->length, 0);
    state->error = state->result < 0 ? errno : 0;
}
static void fastdup_clone_finish(struct fastdup_offload_write_state *state, NTSTATUS status)
{
    struct tevent_req *request = state->request;
    state->queued = false;
    DLIST_REMOVE(fastdup_clone_jobs, state);
    fastdup_clone_pending--;
    if (state->active) { fastdup_clone_active--; }
    close(state->source_fd);
    close(state->target_fd);
    SMB_ASSERT(fastdup_handle_complete(&state->target_state->fence, state->sequence));
    TALLOC_FREE(state->guard);
    talloc_set_destructor(state, NULL);
    if (request == NULL) { TALLOC_FREE(state); return; }
    if (!tevent_req_nterror(request, status)) {
        state->copied = state->result;
        tevent_req_done(request);
    }
}
static void fastdup_clone_done(struct tevent_req *subrequest)
{
    struct fastdup_offload_write_state *state = tevent_req_callback_data(subrequest, struct fastdup_offload_write_state);
    int result = pthreadpool_tevent_job_recv(subrequest);
    NTSTATUS status = NT_STATUS_OK;
    TALLOC_FREE(subrequest);
    /* Thread-creation failure occurs before the syscall; fail explicitly,
     * never turn a failed clone into an event-loop-blocking buffered copy. */
    if (result != 0) {
        status = map_nt_error_from_unix(result);
    } else if (!NT_STATUS_IS_OK(state->validation)) {
        status = state->validation;
    } else if (state->result < 0) {
        status = (state->error == EOPNOTSUPP || state->error == ENOSYS || state->error == EXDEV)
            ? NT_STATUS_INVALID_DEVICE_REQUEST : map_nt_error_from_unix(state->error);
    } else if (state->result != state->length) {
        DBG_ERR("fastdup asynchronous clone returned a forbidden short result\n");
        status = NT_STATUS_IO_DEVICE_ERROR;
    }
    fastdup_clone_finish(state, status);
    fastdup_clone_schedule();
}
static void fastdup_clone_schedule(void)
{
    struct fastdup_offload_write_state *state, *next;
    for (state = fastdup_clone_jobs; state != NULL; state = next) {
        struct fastdup_offload_write_state *earlier;
        struct tevent_req *subrequest;
        bool blocked = false;
        next = state->next;
        if (fastdup_clone_active >= FASTDUP_ASYNC_CLONES) { break; }
        if (state->active) { continue; }
        for (earlier = fastdup_clone_jobs; earlier != state; earlier = earlier->next) {
            if (fastdup_clone_conflict(earlier, state)) { blocked = true; break; }
        }
        if (blocked) { continue; }
        state->active = true;
        fastdup_clone_active++;
        subrequest = pthreadpool_tevent_job_send(state, state->event_context,
            state->handle->conn->sconn->pool, fastdup_clone_do, state);
        if (subrequest == NULL) {
            fastdup_clone_finish(state, NT_STATUS_NO_MEMORY);
            continue;
        }
        tevent_req_set_callback(subrequest, fastdup_clone_done, state);
    }
}


static void fastdup_offload_write_done(struct tevent_req *subrequest)
{
	struct tevent_req *request = tevent_req_callback_data(
		subrequest, struct tevent_req);
	struct fastdup_offload_write_state *state = tevent_req_data(
		request, struct fastdup_offload_write_state);
	NTSTATUS status;

	status = SMB_VFS_NEXT_OFFLOAD_WRITE_RECV(state->handle, subrequest,
						 &state->copied);
	TALLOC_FREE(subrequest);
	if (tevent_req_nterror(request, status)) {
		return;
	}
	tevent_req_done(request);
}

static struct tevent_req *fastdup_offload_write_send(
	struct vfs_handle_struct *handle,
	TALLOC_CTX *memory_context,
	struct tevent_context *event_context,
	uint32_t fsctl,
	DATA_BLOB *token,
	off_t source_offset,
	struct files_struct *target_fsp,
	off_t target_offset,
	off_t to_copy)
{
	struct fastdup_config *config = NULL;
	struct fastdup_offload_write_state *state = NULL;
	struct fastdup_fsp_state *target_state = NULL;
	struct tevent_req *request = NULL;
	struct tevent_req *subrequest = NULL;
	struct files_struct *source_fsp = NULL;
	NTSTATUS status;
	uint64_t sequence;
	bool user_context_changed;

	request = tevent_req_create(memory_context, &state,
				    struct fastdup_offload_write_state);
	if (request == NULL) {
		return NULL;
	}
	state->handle = handle;
	SMB_VFS_HANDLE_GET_DATA(handle, config, struct fastdup_config,
				tevent_req_nterror(request,
						    NT_STATUS_INTERNAL_ERROR);
				return tevent_req_post(request, event_context));

	if (!config->enabled || fsctl != FSCTL_DUP_EXTENTS_TO_FILE) {
		subrequest = SMB_VFS_NEXT_OFFLOAD_WRITE_SEND(
			handle, memory_context, event_context, fsctl, token,
			source_offset, target_fsp, target_offset, to_copy);
		if (tevent_req_nomem(subrequest, request)) {
			return tevent_req_post(request, event_context);
		}
		tevent_req_set_callback(subrequest, fastdup_offload_write_done,
					request);
		return request;
	}

	status = vfs_offload_token_ctx_init(handle->conn->sconn->client,
					    &fastdup_offload_ctx);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	status = vfs_offload_token_db_fetch_fsp(fastdup_offload_ctx, token,
						&source_fsp);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	status = vfs_offload_token_check_handles(fsctl, source_fsp, target_fsp);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
    if (source_offset < 0 || target_offset < 0 || to_copy <= 0) {
        tevent_req_nterror(request, NT_STATUS_INVALID_PARAMETER);
        return tevent_req_post(request, event_context);
    }

    if (fastdup_clone_pending >= FASTDUP_PENDING_CLONES) {
        tevent_req_nterror(request, NT_STATUS_TOO_MANY_COMMANDS);
        return tevent_req_post(request, event_context);
    }
    target_state = fastdup_fsp_state(handle, target_fsp);
    if (target_state == NULL) {
        tevent_req_nterror(request, NT_STATUS_NO_MEMORY);
        return tevent_req_post(request, event_context);
    }
    user_context_changed = change_to_user_and_service_by_fsp(target_fsp);
    if (!user_context_changed) {
        tevent_req_nterror(request, NT_STATUS_INTERNAL_ERROR);
        return tevent_req_post(request, event_context);
    }
    state->source_fd = fcntl(fsp_get_io_fd(source_fsp), F_DUPFD_CLOEXEC, 0);
    if (state->source_fd < 0) {
        tevent_req_nterror(request, map_nt_error_from_unix(errno));
        return tevent_req_post(request, event_context);
    }
    state->target_fd = fcntl(fsp_get_io_fd(target_fsp), F_DUPFD_CLOEXEC, 0);
    if (state->target_fd < 0) {
        status = map_nt_error_from_unix(errno);
        close(state->source_fd);
        tevent_req_nterror(request, status);
        return tevent_req_post(request, event_context);
    }
    /* Pin both Samba handles too: CLOSE waits for this request, and the target
     * extension/fence remains valid until the event-thread completion. The
     * duplicate descriptors independently protect the kernel file lifetime. */
    {
        struct fastdup_clone_guard *guard_state;
        state->guard = tevent_req_create(state, &guard_state, struct fastdup_clone_guard);
    }
    if (state->guard == NULL || !aio_add_req_to_fsp(source_fsp, state->guard) ||
        (source_fsp != target_fsp && !aio_add_req_to_fsp(target_fsp, state->guard))) {
        close(state->source_fd); close(state->target_fd);
        tevent_req_nterror(request, NT_STATUS_NO_MEMORY);
        return tevent_req_post(request, event_context);
    }
    if (!fastdup_handle_accept(&target_state->fence, &sequence)) {
        close(state->source_fd); close(state->target_fd);
        tevent_req_nterror(request, NT_STATUS_TOO_MANY_COMMANDS);
        return tevent_req_post(request, event_context);
    }
    state->request = request;
    state->event_context = event_context;
    state->target_state = target_state;
    state->sequence = sequence;
    state->source_id = source_fsp->file_id;
    state->target_id = target_fsp->file_id;
    state->source_offset = source_offset;
    state->target_offset = target_offset;
    state->length = to_copy;
    state->alignment = config->alignment;
    state->maximum_clone_bytes = config->maximum_clone_bytes;
    state->result = -1;
    talloc_set_destructor(state, fastdup_clone_state_busy);
    state->queued = true;
    tevent_req_set_cleanup_fn(request, fastdup_clone_cleanup);
    DLIST_ADD_END(fastdup_clone_jobs, state);
    fastdup_clone_pending++;
    fastdup_clone_schedule();
    return request;
}

static NTSTATUS fastdup_offload_write_recv(struct vfs_handle_struct *handle,
					   struct tevent_req *request,
					   off_t *copied)
{
	struct fastdup_offload_write_state *state = tevent_req_data(
		request, struct fastdup_offload_write_state);
	NTSTATUS status;

	(void)handle;
	if (tevent_req_is_nterror(request, &status)) {
		tevent_req_received(request);
		return status;
	}
	*copied = state->copied;
	tevent_req_received(request);
	return NT_STATUS_OK;
}

static int fastdup_close(struct vfs_handle_struct *handle,
			 struct files_struct *fsp)
{
	struct fastdup_fsp_state *state = VFS_FETCH_FSP_EXTENSION(handle, fsp);

	if (state != NULL && !fastdup_handle_close_ready(&state->fence)) {
		DBG_ERR("CLOSE attempted before every accepted fastdup metadata "
			"operation was applied: accepted=%" PRIu64
			" applied=%" PRIu64 "\n",
			state->fence.accepted, state->fence.applied);
		SMB_ASSERT(fastdup_handle_close_ready(&state->fence));
		errno = EBUSY;
		return -1;
	}
	return SMB_VFS_NEXT_CLOSE(handle, fsp);
}

static struct vfs_fn_pointers fastdup_fns = {
	.connect_fn = fastdup_connect,
	.close_fn = fastdup_close,
	.fs_capabilities_fn = fastdup_fs_capabilities,
	.fsctl_fn = fastdup_fsctl,
	.offload_read_send_fn = fastdup_offload_read_send,
	.offload_read_recv_fn = fastdup_offload_read_recv,
	.offload_write_send_fn = fastdup_offload_write_send,
	.offload_write_recv_fn = fastdup_offload_write_recv,
};

static_decl_vfs;
NTSTATUS vfs_fastdup_init(TALLOC_CTX *context)
{
	(void)context;
	return smb_register_vfs(SMB_VFS_INTERFACE_VERSION, FASTDUP_MODULE,
				&fastdup_fns);
}
