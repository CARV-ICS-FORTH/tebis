// Copyright (c) Open Enclave SDK contributors.
// Licensed under the MIT License.

#include "common.h"
#include <openenclave/attestation/attester.h>
#include <openenclave/attestation/sgx/evidence.h>
#include <openenclave/attestation/sgx/report.h>
#include <openenclave/attestation/verifier.h>
#include <openenclave/enclave.h>
#include <stdio.h>
#include <string.h>

oe_result_t generate_key_pair(uint8_t **public_key, size_t *public_key_size, uint8_t **private_key,
			      size_t *private_key_size);

int verify_signer_id(const char *siging_public_key_buf, size_t siging_public_key_buf_size, uint8_t *signer_id_buf,
		     size_t signer_id_buf_size);

const oe_claim_t *find_claim(const oe_claim_t *claims, size_t claims_size, const char *name);

oe_result_t load_oe_modules(void);
