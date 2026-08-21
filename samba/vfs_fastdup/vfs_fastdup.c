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
#include "vfs_fastdup_contract.h"

#include <inttypes.h>
#include <unistd.h>

#define FASTDUP_MODULE "fastdup"
#define FASTDUP_DEFAULT_ALIGNMENT ((uint64_t)65536)
#define FASTDUP_DEFAULT_MAX_CLONE ((uint64_t)1073741824)
#define FASTDUP_LINUX_SINGLE_COPY_MAX ((uint64_t)0x7ffff000)
#define FASTDUP_GET_INTEGRITY_BYTES ((uint32_t)16)

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
		if (fsp == NULL || fsp_get_io_fd(fsp) == -1) {
			return NT_STATUS_INVALID_PARAMETER;
		}
		state = fastdup_fsp_state(handle, fsp);
		if (state == NULL) {
			return NT_STATUS_NO_MEMORY;
		}
		if (!fastdup_handle_accept(&state->fence, &sequence)) {
			return NT_STATUS_TOO_MANY_COMMANDS;
		}
		SMB_ASSERT(fastdup_handle_complete(&state->fence, sequence));
		*output_length = 0;
		return NT_STATUS_OK;

	case FSCTL_GET_INTEGRITY_INFORMATION:
		if (fsp == NULL || maximum_output_length < FASTDUP_GET_INTEGRITY_BYTES) {
			return NT_STATUS_INVALID_PARAMETER;
		}
		reply = talloc_zero_array(ctx, uint8_t,
					  FASTDUP_GET_INTEGRITY_BYTES);
		if (reply == NULL) {
			return NT_STATUS_NO_MEMORY;
		}
		contract_status = fastdup_integrity_get_v1(
			reply, FASTDUP_GET_INTEGRITY_BYTES,
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

struct fastdup_offload_write_state {
	struct vfs_handle_struct *handle;
	off_t copied;
};

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
	struct fastdup_clone_request clone_request;
	enum fastdup_contract_status contract_status;
	NTSTATUS status;
	uint64_t sequence;
	off_t source_position = source_offset;
	off_t target_position = target_offset;
	ssize_t copied;
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
	status = vfs_stat_fsp(source_fsp);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	status = vfs_stat_fsp(target_fsp);
	if (tevent_req_nterror(request, status)) {
		return tevent_req_post(request, event_context);
	}
	if (source_offset < 0 || target_offset < 0 || to_copy <= 0) {
		tevent_req_nterror(request, NT_STATUS_INVALID_PARAMETER);
		return tevent_req_post(request, event_context);
	}
	if (source_fsp->fsp_name->st.st_ex_size < 0 ||
	    target_fsp->fsp_name->st.st_ex_size < 0) {
		tevent_req_nterror(request, NT_STATUS_IO_DEVICE_ERROR);
		return tevent_req_post(request, event_context);
	}

	clone_request = (struct fastdup_clone_request) {
		.source_size = source_fsp->fsp_name->st.st_ex_size,
		.target_size = target_fsp->fsp_name->st.st_ex_size,
		.source_offset = (uint64_t)source_offset,
		.target_offset = (uint64_t)target_offset,
		.length = (uint64_t)to_copy,
		.alignment = config->alignment,
		.maximum_length = config->maximum_clone_bytes,
		.same_file = file_id_equal(&source_fsp->file_id,
					   &target_fsp->file_id),
	};
	contract_status = fastdup_validate_clone_v1(&clone_request);
	if (contract_status != FASTDUP_CONTRACT_OK) {
		tevent_req_nterror(request,
				   fastdup_contract_ntstatus(contract_status));
		return tevent_req_post(request, event_context);
	}

	target_state = fastdup_fsp_state(handle, target_fsp);
	if (target_state == NULL) {
		tevent_req_nterror(request, NT_STATUS_NO_MEMORY);
		return tevent_req_post(request, event_context);
	}
	if (!fastdup_handle_accept(&target_state->fence, &sequence)) {
		tevent_req_nterror(request, NT_STATUS_TOO_MANY_COMMANDS);
		return tevent_req_post(request, event_context);
	}

	user_context_changed = change_to_user_and_service_by_fsp(target_fsp);
	if (!user_context_changed) {
		SMB_ASSERT(fastdup_handle_complete(&target_state->fence, sequence));
		tevent_req_nterror(request, NT_STATUS_INTERNAL_ERROR);
		return tevent_req_post(request, event_context);
	}
	copied = copy_file_range(fsp_get_io_fd(source_fsp), &source_position,
				 fsp_get_io_fd(target_fsp), &target_position,
				 (size_t)to_copy, 0);
	SMB_ASSERT(fastdup_handle_complete(&target_state->fence, sequence));
	if (copied < 0) {
		status = (errno == EOPNOTSUPP || errno == ENOSYS || errno == EXDEV)
			       ? NT_STATUS_INVALID_DEVICE_REQUEST
			       : map_nt_error_from_unix(errno);
		tevent_req_nterror(request, status);
		return tevent_req_post(request, event_context);
	}
	if (copied != to_copy) {
		DBG_ERR("fastdup copy_file_range returned a forbidden short clone "
			"requested=%jd copied=%jd\n",
			(intmax_t)to_copy, (intmax_t)copied);
		tevent_req_nterror(request, NT_STATUS_IO_DEVICE_ERROR);
		return tevent_req_post(request, event_context);
	}
	state->copied = copied;
	tevent_req_done(request);
	return tevent_req_post(request, event_context);
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
