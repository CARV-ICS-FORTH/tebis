#include <mbedtls/ctr_drbg.h>
#include <mbedtls/pk.h>
#include <mbedtls/rsa.h>
#include <mbedtls/sha256.h>
#include <mbedtls/x509_crt.h>

int generate_certificate_and_pkey(mbedtls_x509_crt *certificate, mbedtls_pk_context *private_key);
