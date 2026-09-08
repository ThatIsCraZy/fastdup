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

static void integrity_wire_validation_and_default_none_state(void)
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
	       FASTDUP_CONTRACT_OK);

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

	assert(fastdup_integrity_get_v1(NULL, sizeof(reply), FASTDUP_CHECKSUM_NONE, 65536,
					&reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), FASTDUP_CHECKSUM_NONE, 65536, NULL) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), FASTDUP_CHECKSUM_NONE, 6144,
					&reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, 15, FASTDUP_CHECKSUM_NONE, 65536, &reply_length) ==
	       FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, sizeof(reply), FASTDUP_CHECKSUM_NONE, 65536,
					&reply_length) == FASTDUP_CONTRACT_OK);
	assert(reply_length == sizeof(reply));
	assert(reply[0] == 0 && reply[1] == 0);
	assert(reply[2] == 0 && reply[3] == 0);
	assert(load_u32_le(reply + 4) == 0);
	assert(load_u32_le(reply + 8) == 0);
	assert(load_u32_le(reply + 12) == 65536);
}

static void integrity_enable_roundtrips_and_unchanged_preserves_state(void)
{
	uint8_t request[8] = {0};
	uint8_t stored[2] = {0};
	uint8_t reply[16];
	uint16_t algorithm;
	size_t produced;
	for (uint16_t requested = 1; requested <= 2; requested++) {
		store_u16_le(request, requested);
		assert(fastdup_integrity_resolve_v1(request, 8, 0, 65536, stored) == FASTDUP_CONTRACT_OK);
		assert(stored[0] == 2 && stored[1] == 0);
		assert(fastdup_integrity_decode_v1(stored, 2, &algorithm) == FASTDUP_CONTRACT_OK);
		assert(fastdup_integrity_get_v1(reply, 16, algorithm, 65536, &produced) == FASTDUP_CONTRACT_OK);
		assert(reply[0] == 2 && load_u32_le(reply + 8) == 65536);
		store_u16_le(request, FASTDUP_CHECKSUM_UNCHANGED);
		assert(fastdup_integrity_resolve_v1(request, 8, algorithm, 65536, stored) == FASTDUP_CONTRACT_OK);
		assert(stored[0] == 2 && stored[1] == 0);
	}
	store_u16_le(request, 2);
	assert(fastdup_integrity_resolve_v1(request, 8, 0, 4096, stored) == FASTDUP_CONTRACT_OK);
	assert(stored[0] == 1);
	store_u16_le(request, 0);
	assert(fastdup_integrity_resolve_v1(request, 8, 2, 65536, stored) == FASTDUP_CONTRACT_OK);
	assert(stored[0] == 0 && stored[1] == 0);
	assert(fastdup_integrity_decode_v1(stored, 1, &algorithm) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	stored[0] = 3;
	assert(fastdup_integrity_decode_v1(stored, 2, &algorithm) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	stored[0] = 0; stored[1] = 1;
	assert(fastdup_integrity_decode_v1(stored, 2, &algorithm) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_get_v1(reply, 16, 3, 65536, &produced) == FASTDUP_CONTRACT_INVALID_PARAMETER);
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
	       FASTDUP_CONTRACT_OK);
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

/* Veeam also sends a partial final cluster inside a larger source file. */
static void veeam_consecutive_byte_ranges_are_admitted(void)
{
	struct fastdup_clone_request request = {
		.source_size = 19907710976ULL, .target_size = 4194304,
		.source_offset = 1625088, .target_offset = 1616896,
		.length = 5120, .alignment = FASTDUP_CLONE_ALIGNMENT_V1,
		.maximum_length = 1073741824, .same_file = false,
	};
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	/* Every starting byte residue, independently on both sides; exact EOF. */
	for (uint64_t residue = 0; residue < 4096; residue++) {
		request.source_offset = 1617920 + residue;
		request.target_offset = 1609728 + (residue * 17 % 4096);
		request.length = residue + 1;
		request.source_size = request.source_offset + request.length;
		request.target_size = request.target_offset + request.length;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
		request.source_size--;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS);
		request.source_size++;
		request.target_size--;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_TARGET_NOT_PRESIZED);
	}
}

static void veeam_partial_cluster_lengths_preserve_exact_bounds(void)
{
	struct fastdup_clone_request request = {
		.source_size = 19907710976ULL, .target_size = 4194304,
		.source_offset = 1617920, .target_offset = 1609728,
		.length = 7168, .alignment = FASTDUP_CLONE_ALIGNMENT_V1,
		.maximum_length = 1073741824, .same_file = false,
	};
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	/* No rounding, sector-size special case, or source-EOF exception. */
	for (uint64_t length = 1; length <= 8193; length++) {
		request.length = length;
		request.source_size = request.source_offset + length;
		request.target_size = request.target_offset + length;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
		request.source_size--;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS);
		request.source_size++;
		request.target_size--;
		assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_TARGET_NOT_PRESIZED);
	}
	request.source_size = request.target_size = UINT64_MAX;
	request.length = request.maximum_length;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	request.length++;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_CLONE_TOO_LARGE);
	request.length = 7168;
	request.source_offset = UINT64_MAX - 4095;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	request.source_offset = 1617920;
	request.target_offset++;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	request.target_offset = request.source_offset + 4096;
	request.same_file = true;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OVERLAP);
}

static void veeam_8192_byte_clone_and_legacy_integrity_are_compatible(void)
{
	struct fastdup_clone_request request = {
		.source_size = 10753359872ULL, .target_size = 4194304,
		.source_offset = 1617920, .target_offset = 1609728,
		.length = 8192, .alignment = FASTDUP_CLONE_ALIGNMENT_V1,
		.maximum_length = 1073741824, .same_file = false,
	};
	uint8_t old_policy[2] = {2, 0}, new_policy[2] = {1, 0};
	uint8_t reply[16], unchanged[8] = {0xff, 0xff}, stored[2];
	uint16_t old_algorithm, new_algorithm;
	size_t length;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	request.alignment = 65536;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	request.alignment = FASTDUP_CLONE_ALIGNMENT_V1;
	request.target_offset++;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_OK);
	request.target_offset--;
	request.target_size = request.target_offset + request.length - 1;
	assert(fastdup_validate_clone_v1(&request) == FASTDUP_CONTRACT_TARGET_NOT_PRESIZED);

	assert(fastdup_integrity_decode_v1(old_policy, 2, &old_algorithm) == FASTDUP_CONTRACT_OK);
	assert(fastdup_integrity_decode_v1(new_policy, 2, &new_algorithm) == FASTDUP_CONTRACT_OK);
	assert(fastdup_integrity_effective_v1(old_algorithm, 4096, &old_algorithm) == FASTDUP_CONTRACT_OK);
	assert(fastdup_integrity_effective_v1(new_algorithm, 4096, &new_algorithm) == FASTDUP_CONTRACT_OK);
	assert(old_algorithm == new_algorithm && old_algorithm == FASTDUP_CHECKSUM_CRC32);
	assert(old_policy[0] == 2); /* Interpretation does not mutate stored metadata. */
	assert(fastdup_integrity_get_v1(reply, 16, 2, 4096, &length) == FASTDUP_CONTRACT_OK);
	assert(reply[0] == 1 && load_u32_le(reply + 8) == 4096 && load_u32_le(reply + 12) == 4096);
	assert(fastdup_integrity_effective_v1(0, 4096, &new_algorithm) == FASTDUP_CONTRACT_OK);
	assert(old_algorithm != new_algorithm); /* Enabled/NONE still cannot clone. */
	assert(fastdup_integrity_resolve_v1(unchanged, 8, 2, 4096, stored) == FASTDUP_CONTRACT_OK);
	assert(stored[0] == 2); /* UNCHANGED retains the stored representation. */
	assert(fastdup_integrity_effective_v1(3, 4096, &new_algorithm) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_effective_v1(2, 6144, &new_algorithm) == FASTDUP_CONTRACT_INVALID_PARAMETER);
	assert(fastdup_integrity_effective_v1(2, 4096, NULL) == FASTDUP_CONTRACT_INVALID_PARAMETER);
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
	integrity_wire_validation_and_default_none_state();
	integrity_enable_roundtrips_and_unchanged_preserves_state();
	duplicate_extents_is_one_bounded_presized_operation();
	veeam_8192_byte_clone_and_legacy_integrity_are_compatible();
	veeam_partial_cluster_lengths_preserve_exact_bounds();
	veeam_consecutive_byte_ranges_are_admitted();
	close_is_fenced_by_every_accepted_metadata_operation();
	return 0;
}
