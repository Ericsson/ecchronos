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
# Certs for CQL
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
# Certs for REST
# cert/serverca.crt - REST CA
# cert/servercakey.pem - REST CA private key
# cert/server.csr - REST server CSR
# cert/servercert.crt - REST server cert
# cert/serverkey.pem - REST server private key
# cert/serverkeystore - REST server keystore (PKCS12)
# cert/servertruststore - REST server truststore (PKCS12)
#
# Certs for ecctool
# cert/clientcert.crt - ecctool certificate
# cert/clientkey.pem - ecctool private key
# cert/client.csr
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

SERVER_CA="cert/serverca.crt"
SERVER_CA_KEY="cert/servercakey.pem"
SERVER_CSR="cert/server.csr"
SERVER_CERT="cert/servercert.crt"
SERVER_CERT_KEY="cert/serverkey.pem"
SERVER_KEYSTORE="cert/serverkeystore"
SERVER_TRUSTSTORE="cert/servertruststore"

ECCTOOL_CLIENT_CA="cert/clientca.crt"
ECCTOOL_CLIENT_CA_KEY="cert/clientcakey.pem"
ECCTOOL_CLIENT_CERT="cert/clientcert.crt"
ECCTOOL_CLIENT_CERT_KEY="cert/clientkey.pem"
ECCTOOL_CLIENT_CSR="cert/client.csr"

######################
# Generate Server CA #
######################

## Generate self-signed CA (this is the CA that the client should trust ad that we should issue server certificates from)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=RESTSERVERCA"\
 -keyout "$SERVER_CA_KEY" -out "$SERVER_CA"

###############################
# Generate server certificate #
###############################

## Generate server key and CSR
openssl req -newkey rsa:2048 -nodes -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=localhost"\
 -keyout "$SERVER_CERT_KEY" -out "$SERVER_CSR"

# Sign server certificate
openssl x509 -req -sha256 -days 1\
 -in "$SERVER_CSR" -out "$SERVER_CERT"\
 -extfile <(printf "subjectAltName=DNS:localhost")\
 -CA "$SERVER_CA" -CAkey "$SERVER_CA_KEY" -CAcreateserial

## Convert server key/certificate to PKCS12 format
openssl pkcs12 -export\
 -in "$SERVER_CERT" -inkey "$SERVER_CERT_KEY"\
 -out "$SERVER_KEYSTORE" -passout pass:"ecctest"

##############################
# Generate ecctool client CA #
##############################

## Generate self-signed client CA (this is the CA that the server should trust and that we should issue client certificates from)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=RESTCLIENTCA"\
 -keyout "$ECCTOOL_CLIENT_CA_KEY" -out "$ECCTOOL_CLIENT_CA"

## Import self-signed client CA certificate to server truststore
keytool -importcert -noprompt\
 -file "$ECCTOOL_CLIENT_CA"\
 -keystore "$SERVER_TRUSTSTORE" -storepass "ecctest"

#######################################
# Generate ecctool client certificate #
#######################################

## Generate ecctool key and CSR
openssl req -newkey rsa:2048 -nodes -subj "/C=TE/ST=TEST/L=TEST/O=TEST/OU=TEST/CN=ecctool"\
 -keyout "$ECCTOOL_CLIENT_CERT_KEY" -out "$ECCTOOL_CLIENT_CSR"

# Sign ecctool certificate
openssl x509 -req -sha256 -days 1\
 -in "$ECCTOOL_CLIENT_CSR" -out "$ECCTOOL_CLIENT_CERT"\
 -CA "$ECCTOOL_CLIENT_CA" -CAkey "$ECCTOOL_CLIENT_CA_KEY" -CAcreateserial
