#include "vfs_fastdup_contract.h"

#include <limits.h>

#define FASTDUP_SET_INTEGRITY_BYTES ((size_t)8)
#define FASTDUP_GET_INTEGRITY_BYTES ((size_t)16)

static uint16_t load_u16_le(const uint8_t *bytes)
{
	return (uint16_t)((uint16_t)bytes[0] |
			  (uint16_t)((uint16_t)bytes[1] << 8));
}

static uint32_t load_u32_le(const uint8_t *bytes)
{
	return (uint32_t)bytes[0] |
	       ((uint32_t)bytes[1] << 8) |
	       ((uint32_t)bytes[2] << 16) |
	       ((uint32_t)bytes[3] << 24);
}

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

static bool is_power_of_two_u64(uint64_t value)
{
	return value != 0 && (value & (value - 1)) == 0;
}

enum fastdup_contract_status fastdup_integrity_set_v1(const uint8_t *input,
						       size_t input_length)
{
	uint16_t algorithm;
	uint32_t flags;

	if (input == NULL || input_length < FASTDUP_SET_INTEGRITY_BYTES) {
		return FASTDUP_CONTRACT_INVALID_PARAMETER;
	}

	algorithm = load_u16_le(input);
	flags = load_u32_le(input + 4);
	if (flags != 0) {
		return FASTDUP_CONTRACT_INVALID_PARAMETER;
	}
	if (algorithm == FASTDUP_CHECKSUM_NONE ||
	    algorithm == FASTDUP_CHECKSUM_UNCHANGED) {
		return FASTDUP_CONTRACT_OK;
	}
	if (algorithm == FASTDUP_CHECKSUM_CRC32 ||
	    algorithm == FASTDUP_CHECKSUM_CRC64) {
		return FASTDUP_CONTRACT_UNSUPPORTED_INTEGRITY_STATE;
	}
	return FASTDUP_CONTRACT_INVALID_PARAMETER;
}

enum fastdup_contract_status fastdup_integrity_get_v1(uint8_t *output,
						       size_t output_capacity,
						       uint32_t cluster_size,
						       size_t *output_length)
{
	if (output == NULL || output_length == NULL ||
	    output_capacity < FASTDUP_GET_INTEGRITY_BYTES ||
	    !is_power_of_two_u64(cluster_size) || cluster_size < 4096) {
		return FASTDUP_CONTRACT_INVALID_PARAMETER;
	}

	store_u16_le(output, FASTDUP_CHECKSUM_NONE);
	store_u16_le(output + 2, 0);
	store_u32_le(output + 4, 0);
	store_u32_le(output + 8, 0);
	store_u32_le(output + 12, cluster_size);
	*output_length = FASTDUP_GET_INTEGRITY_BYTES;
	return FASTDUP_CONTRACT_OK;
}

enum fastdup_contract_status fastdup_validate_clone_v1(
	const struct fastdup_clone_request *request)
{
	uint64_t source_end;
	uint64_t target_end;

	if (request == NULL || request->length == 0 ||
	    !is_power_of_two_u64(request->alignment) ||
	    request->alignment < 4096 || request->maximum_length == 0) {
		return FASTDUP_CONTRACT_INVALID_PARAMETER;
	}
	if (request->length > request->maximum_length) {
		return FASTDUP_CONTRACT_CLONE_TOO_LARGE;
	}
	if ((request->source_offset & (request->alignment - 1)) != 0 ||
	    (request->target_offset & (request->alignment - 1)) != 0 ||
	    (request->length & (request->alignment - 1)) != 0) {
		return FASTDUP_CONTRACT_MISALIGNED;
	}
	if (request->source_offset > UINT64_MAX - request->length ||
	    request->target_offset > UINT64_MAX - request->length) {
		return FASTDUP_CONTRACT_INVALID_PARAMETER;
	}
	source_end = request->source_offset + request->length;
	target_end = request->target_offset + request->length;
	if (source_end > request->source_size) {
		return FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS;
	}
	if (target_end > request->target_size) {
		return FASTDUP_CONTRACT_TARGET_NOT_PRESIZED;
	}
	if (request->same_file && request->source_offset < target_end &&
	    request->target_offset < source_end) {
		return FASTDUP_CONTRACT_OVERLAP;
	}
	return FASTDUP_CONTRACT_OK;
}

bool fastdup_handle_accept(struct fastdup_handle_fence *fence,
			   uint64_t *sequence)
{
	if (fence == NULL || sequence == NULL || fence->accepted == UINT64_MAX) {
		return false;
	}
	fence->accepted++;
	*sequence = fence->accepted;
	return true;
}

bool fastdup_handle_complete(struct fastdup_handle_fence *fence,
			     uint64_t sequence)
{
	if (fence == NULL || sequence != fence->applied + 1 ||
	    sequence > fence->accepted) {
		return false;
	}
	fence->applied = sequence;
	return true;
}

bool fastdup_handle_close_ready(const struct fastdup_handle_fence *fence)
{
	return fence != NULL && fence->accepted == fence->applied;
}
