#!/bin/bash
#
# Copyright 2020 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Output:
# (ca/key.pem - CA private key)
# cert/ca.crt - CA certificate
# cert/.truststore - JKS (CA) truststore
#
# cert/key.pem - User private key
# cert/cert.crt - User certificate
# (cert/cert.csr - User certificate sign request)
# (cert/cert.pkcs12 - User cert in PKCS#12 format)
# cert/.keystore - JKS (user) keystore
#

mkdir -p ca
mkdir -p cert

CA_KEY="ca/key.pem"
CA_CERT="cert/ca.crt"
TRUSTSTORE="cert/.truststore"

USER_KEY="cert/key.pem"
USER_CERT="cert/cert.crt"
USER_CSR="cert/cert.csr"
USER_PKCS12="cert/cert.pkcs12"
KEYSTORE="cert/.keystore"


###############
# Generate CA #
###############

## Generate self-signed CA
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=CA"\
 -keyout "$CA_KEY" -out "$CA_CERT"

## Import self-signed CA certificate to truststore
keytool -importcert -noprompt\
 -file "$CA_CERT"\
 -keystore "$TRUSTSTORE" -storepass "ecctest"


########################
# Generate certificate #
########################

## Generate user key and CSR
openssl req -newkey rsa:2048 -nodes -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=User"\
 -keyout "$USER_KEY" -out "$USER_CSR"

# Sign user certificate
openssl x509 -req -sha256 -days 1\
 -in "$USER_CSR" -out "$USER_CERT"\
 -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial

## Convert key/certificate to PKCS12 format
openssl pkcs12 -export\
 -in "$USER_CERT" -inkey "$USER_KEY"\
 -out "$USER_PKCS12" -passout pass:"ecctest"

## Import PKCS12 key to keystore
keytool -importkeystore\
 -srckeystore "$USER_PKCS12" -srcalias "1" -srcstorepass "ecctest"\
 -destkeystore "$KEYSTORE" -destalias "cert" -deststorepass "ecctest"
