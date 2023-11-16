// Copyright (c) Open Enclave SDK contributors.
// Licensed under the MIT License.

#include "mbedtls_utility.h"
#include "testcerts.h"
#include <mbedtls/ctr_drbg.h>
#include <openenclave/attestation/attester.h>
#include <openenclave/attestation/sgx/evidence.h>
#include <openenclave/attestation/sgx/report.h>

// SGX Remote Attestation UUID.
// static oe_uuid_t _uuid_sgx_ecdsa = {OE_FORMAT_UUID_SGX_ECDSA};

const unsigned char certificate_subject_name[] = "CN=Open Enclave SDK,O=OESDK TLS,C=US";

static const char test_srv_key[] = TEST_SRV_KEY_RSA_PEM;
const char *mbedtls_test_srv_key = test_srv_key;
const size_t mbedtls_test_srv_key_len = sizeof(test_srv_key);

#define TEST_SRV_CRT TEST_SRV_CRT_RSA_SHA256_PEM

const char mbedtls_test_cas_pem[] = TEST_CA_CRT_RSA_SHA256_PEM;
const size_t mbedtls_test_cas_pem_len = sizeof(mbedtls_test_cas_pem);

// Consider to move this function into a shared directory
oe_result_t generate_certificate_and_pkey(mbedtls_x509_crt *certificate, mbedtls_pk_context *private_key)
{
	oe_result_t result = OE_FAILURE;
	uint8_t *private_key_buffer = NULL;
	size_t private_key_buffer_size = 0;
	uint8_t *public_key_buffer = NULL;
	size_t public_key_buffer_size = 0;
	// uint8_t* optional_parameters = NULL;
	// size_t optional_parameters_size = 0;
	int ret = 0;

	// result = generate_key_pair(&public_key_buffer, &public_key_buffer_size,
	//                            &private_key_buffer, &private_key_buffer_size);
	// if (result != OE_OK) {
	//   printf("generate_key_pair failed with %s\n", oe_result_str(result));
	//   goto exit;
	// }

	// printf("public key used:\n%s\n", public_key_buffer);

	// both ec key such ASYMMETRIC_KEY_EC_SECP256P1 or RSA key work
	// oe_attester_initialize();
	// result = oe_get_attestation_certificate_with_evidence_v2(
	//    &_uuid_sgx_ecdsa,
	//    certificate_subject_name,
	//    private_key_buffer,
	//    private_key_buffer_size,
	//    public_key_buffer,
	//    public_key_buffer_size,
	//    optional_parameters,
	//    optional_parameters_size,
	//    &output_certificate,
	//    &output_certificate_size);
	// if (result != OE_OK)
	//{
	//    printf(
	//        "oe_get_attestation_certificate_with_evidence_v2 failed with %s\n",
	//        oe_result_str(result));
	//    goto exit;
	//}
	/*
   * This demonstration program uses embedded test certificates.
   * Instead, you may want to use mbedtls_x509_crt_parse_file() to read the
   * server and CA certificates, as well as mbedtls_pk_parse_keyfile().
   */
	static const char test_srv_crt[] = TEST_SRV_CRT;
	const char *mbedtls_test_srv_crt = test_srv_crt;
	const size_t mbedtls_test_srv_crt_len = sizeof(test_srv_crt);
	ret = mbedtls_x509_crt_parse(certificate, (const unsigned char *)mbedtls_test_srv_crt,
				     mbedtls_test_srv_crt_len);
	if (ret != 0) {
		printf(" failed\n  !  mbedtls_x509_crt_parse returned -0x%x\n\n", ret);
		goto exit;
	}

	ret = mbedtls_x509_crt_parse(certificate, (const unsigned char *)mbedtls_test_cas_pem,
				     mbedtls_test_cas_pem_len);
	if (ret != 0) {
		printf(" failed\n  !  mbedtls_x509_crt_parse (2) returned -0x%x\n\n", ret);
		goto exit;
	}

	ret = mbedtls_pk_parse_key(private_key, (const unsigned char *)mbedtls_test_srv_key, mbedtls_test_srv_key_len,
				   NULL, 0);
	if (ret != 0) {
		printf(" failed\n  !  mbedtls_pk_parse_key returned %d\n\n", ret);
		goto exit;
	}

	////create mbedtls_x509_crt from output_cert
	// ret = mbedtls_x509_crt_parse_der(
	//     certificate, output_certificate, output_certificate_size);
	// if (ret != 0)
	//{
	//     printf("mbedtls_x509_crt_parse_der failed with ret = %d\n", ret);
	//     result = OE_FAILURE;
	//     goto exit;
	// }

	//// create mbedtls_pk_context from private key data
	// ret = mbedtls_pk_parse_key(
	//     private_key,
	//     (const unsigned char*)private_key_buffer,
	//     private_key_buffer_size,
	//     NULL,
	//     0);
	// if (ret != 0)
	//{
	//     printf("mbedtls_pk_parse_key failed with ret = %d\n", ret);
	//     result = OE_FAILURE;
	//     goto exit;
	// }
	result = OE_OK;
exit:
	// oe_attester_shutdown();
	oe_free_key(private_key_buffer, private_key_buffer_size, NULL, 0);
	oe_free_key(public_key_buffer, public_key_buffer_size, NULL, 0);
	// oe_free_attestation_certificate(output_certificate);
	return result;
}
