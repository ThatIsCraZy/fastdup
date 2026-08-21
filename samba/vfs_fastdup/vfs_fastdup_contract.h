#ifndef FASTDUP_VFS_CONTRACT_H
#define FASTDUP_VFS_CONTRACT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define FASTDUP_CHECKSUM_NONE ((uint16_t)0x0000)
#define FASTDUP_CHECKSUM_CRC32 ((uint16_t)0x0001)
#define FASTDUP_CHECKSUM_CRC64 ((uint16_t)0x0002)
#define FASTDUP_CHECKSUM_UNCHANGED ((uint16_t)0xffff)
#define FASTDUP_INTEGRITY_ENFORCEMENT_OFF ((uint32_t)0x00000001)

enum fastdup_contract_status {
	FASTDUP_CONTRACT_OK = 0,
	FASTDUP_CONTRACT_INVALID_PARAMETER,
	FASTDUP_CONTRACT_UNSUPPORTED_INTEGRITY_STATE,
	FASTDUP_CONTRACT_TARGET_NOT_PRESIZED,
	FASTDUP_CONTRACT_MISALIGNED,
	FASTDUP_CONTRACT_CLONE_TOO_LARGE,
	FASTDUP_CONTRACT_OVERLAP,
	FASTDUP_CONTRACT_SOURCE_OUT_OF_BOUNDS
};

struct fastdup_clone_request {
	uint64_t source_size;
	uint64_t target_size;
	uint64_t source_offset;
	uint64_t target_offset;
	uint64_t length;
	uint64_t alignment;
	uint64_t maximum_length;
	bool same_file;
};

/*
 * Version 1 deliberately exposes one immutable Integrity Information state:
 * CHECKSUM_TYPE_NONE with enforcement enabled. SET succeeds only when it
 * leaves that state unchanged. This is persistent by definition and cannot
 * drift from the state returned by GET after a restart.
 */
enum fastdup_contract_status fastdup_integrity_set_v1(const uint8_t *input,
						       size_t input_length);

enum fastdup_contract_status fastdup_integrity_get_v1(uint8_t *output,
						       size_t output_capacity,
						       uint32_t cluster_size,
						       size_t *output_length);

enum fastdup_contract_status fastdup_validate_clone_v1(
	const struct fastdup_clone_request *request);

/*
 * Accepted and applied sequence numbers are per Samba open handle. The
 * adapter must not invoke the next close hook until they are equal.
 */
struct fastdup_handle_fence {
	uint64_t accepted;
	uint64_t applied;
};

bool fastdup_handle_accept(struct fastdup_handle_fence *fence,
			   uint64_t *sequence);
bool fastdup_handle_complete(struct fastdup_handle_fence *fence,
			     uint64_t sequence);
bool fastdup_handle_close_ready(const struct fastdup_handle_fence *fence);

#endif
