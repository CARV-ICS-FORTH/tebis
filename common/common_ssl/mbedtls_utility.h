// Copyright (c) Open Enclave SDK contributors.
// Licensed under the MIT License.

#include "utility.h"
#include <mbedtls/ctr_drbg.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>
#include <mbedtls/x509_crt.h>
#include <openenclave/enclave.h>

oe_result_t generate_certificate_and_pkey(mbedtls_x509_crt *certificate, mbedtls_pk_context *private_key);
