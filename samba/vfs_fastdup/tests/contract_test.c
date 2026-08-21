#include "../vfs_fastdup_contract.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void store_u16_le(uint8_t *bytes, uint16_t value)
{
	bytes[0] = (uint8_t)value;
	bytes[1] = (uint8_t)(value >> 8);
}

static void store_u32_le(uint8_t *bytes, uint32_t value)
{
	bytes[0] = (uint8_t)value;
	bytes[1] = (uint8_t)(value >> 8);
	bytes[2] = (uint8_t)(value >> 16);
	bytes[3] = (uint8_t)(value >> 24);
}

static uint32_t load_u32_le(const uint8_t *bytes)
{
	return (uint32_t)bytes[0] |
	       ((uint32_t)bytes[1] << 8) |
	       ((uint32_t)bytes[2] << 16) |
	       ((uint32_t)bytes[3] << 24);
}

static void integrity_v1_is_a_fixed_consistent_none_state(void)
{
	uint8_t request[12] = {0};
	uint8_t reply[16] = {0xff};
	size_t reply_length = 0;

	assert(fastdup_integrity_set_v1(NULL, 8) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_set_v1(request, 7) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_set_v1(request, 8) == FASTDUP_CONTRACT_OK);

	store_u16_le(request, FASTDUP_CHECKSUM_UNCHANGED);
	request[2] = 0x55;
	request[3] = 0xaa;
	assert(fastdup_integrity_set_v1(request, sizeof(request)) ==
	       FASTDUP_CONTRACT_OK);

	store_u16_le(request, FASTDUP_CHECKSUM_CRC64);
	assert(fastdup_integrity_set_v1(request, 8) ==
	       FASTDUP_CONTRACT_UNSUPPORTED_INTEGRITY_STATE);

	store_u16_le(request, FASTDUP_CHECKSUM_NONE);
	store_u32_le(request + 4, FASTDUP_INTEGRITY_ENFORCEMENT_OFF);
	assert(fastdup_integrity_set_v1(request, 8) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);

	store_u16_le(request, FASTDUP_CHECKSUM_UNCHANGED);
	assert(fastdup_integrity_set_v1(request, 8) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);

	store_u32_le(request + 4, 2);
	assert(fastdup_integrity_set_v1(request, 8) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);

	assert(fastdup_integrity_get_v1(NULL, sizeof(reply), 65536,
					&reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), 65536, NULL) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), 6144,
					&reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, 15, 65536, &reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), 65536,
					&reply_length) == FASTDUP_CONTRACT_OK);
	assert(reply_length == sizeof(reply));
	assert(reply[0] == 0 && reply[1] == 0);
	assert(reply[2] == 0 && reply[3] == 0);
	assert(load_u32_le(reply + 4) == 0);
	assert(load_u32_le(reply + 8) == 0);
	assert(load_u32_le(reply + 12) == 65536);
}

static void duplicate_extents_is_one_bounded_presized_operation(void)
{
	struct fastdup_clone_request request = {
		.source_size = 1024 * 1024,
		.target_size = 1024 * 1024,
		.source_offset = 65536,
		.target_offset = 131072,
		.length = 262144,
		.alignment = 65536,
		.maximum_length = 1024 * 1024,
		.same_file = false,
	};

	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);

	request.target_size = request.target_offset + request.length - 1;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_TARGET_NOT_PRESIZED);
	request.target_size = 1024 * 1024;

	request.source_offset++;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_MISALIGNED);
	request.source_offset--;

	request.length = request.maximum_length + request.alignment;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_CLONE_TOO_LARGE);
	request.length = 262144;

	request.same_file = true;
	request.target_offset = request.source_offset + request.alignment;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_OVERLAP);
	request.target_offset = request.source_offset + request.length;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);

	request.same_file = false;
	request.source_offset = UINT64_MAX - request.length + 1;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	request.source_offset = 0;
	request.target_offset = UINT64_MAX - request.length + 1;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);

	request.target_offset = 0;
	request.source_size = request.length - 1;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS);
	request.source_size = request.length;
	request.target_size = request.length;
	request.alignment = 6144;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	request.alignment = 65536;
	request.length = 0;
	assert(fastdup_validate_clone_v1(&request) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_validate_clone_v1(NULL) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
}

static void close_is_fenced_by_every_accepted_metadata_operation(void)
{
	struct fastdup_handle_fence fence = {0};
	uint64_t first;
	uint64_t second;

	assert(fastdup_handle_close_ready(&fence));
	assert(fastdup_handle_accept(&fence, &first));
	assert(fastdup_handle_accept(&fence, &second));
	assert(first == 1);
	assert(second == 2);
	assert(!fastdup_handle_close_ready(&fence));
	assert(!fastdup_handle_complete(&fence, second));
	assert(fastdup_handle_complete(&fence, first));
	assert(!fastdup_handle_close_ready(&fence));
	assert(fastdup_handle_complete(&fence, second));
	assert(fastdup_handle_close_ready(&fence));
	assert(!fastdup_handle_complete(&fence, second));
	assert(!fastdup_handle_close_ready(NULL));
	assert(!fastdup_handle_accept(NULL, &first));
	fence.accepted = UINT64_MAX;
	fence.applied = UINT64_MAX;
	assert(!fastdup_handle_accept(&fence, &first));
	assert(fastdup_handle_close_ready(&fence));
}

int main(void)
{
	integrity_v1_is_a_fixed_consistent_none_state();
	duplicate_extents_is_one_bounded_presized_operation();
	close_is_fenced_by_every_accepted_metadata_operation();
	return 0;
}
