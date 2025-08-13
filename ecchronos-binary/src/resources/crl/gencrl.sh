#!/bin/bash
#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
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

# Simple information on tool usage
#
TOOL=$(basename $0)
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "Usage: ${TOOL} [OPTION] [DIRECTORY]"
    echo "Generate a set of certificates and CRL files for testing CRL."
    echo ""
    echo "    -h, --help   This help text."
    echo "    DIRECTORY    The working directory. Default is 'work'."
    echo ""
    echo "To create a single set of test files, run the tool once:"
    echo "  > ${TOOL} [DIRECTORY]"
    echo ""
    echo "  The result is found in [DIRECTORY]."
    echo ""
    echo "If test files with a CRL containing both non-revoked and revoked certificates"
    echo "is needed, run the tool twice and combine them:"
    echo "  > ${TOOL} [DIRECTORY1]"
    echo "  > ${TOOL} [DIRECTORY2]"
    echo "  > cat [DIRECTORY1]/crl/*-nothing-revoked.crl [DIRECTORY2]/crl/*-revoked.crl > [DIRECTORY2]/crl/multiple.crl"
    echo "  > rm -rf [DIRECTORY1]"
    echo ""
    echo "  The resulting files is now found in [DIRECTORY2]."
    echo ""
    echo "Observe: CRL and CRT files can also be named to PEM as needed. A CRL file"
    echo "may contain none or a multiple of CRL entries."
    echo ""
    echo "To examine generated certificate and CRL files, use the following:"
    echo "  > openssl x509 -text -noout -in [FILE.<crt|pem>]"
    echo "  > openssl crl -text -noout -in [FILE.crl]"
    echo ""
    echo "Observe: When viewing files this way, ONLY the first entry will be output. The"
    echo "way in which the multiple file was done, does matter in this specific case."
    exit 0
fi

# Constants
#
REV=1                                     # Revision of the tool
DIR=${1:-./work}                          # Working root directory
CN_CLIENT="client"                        # Common name for client
CN_SERVER="server"                        # Common name for server
CA="root-ca"                              # CA name
DAYS=3650                                 # Ten years validity
ENCRYPTION="rsa:4096"                     # Encryption to use
INDEX="${DIR}/ca/${CA}/db/${CA}.db"       # Index file
SERIAL="${DIR}/ca/${CA}/db/${CA}.crt.srl" # Serial number file
CRLNUM="${DIR}/ca/${CA}/db/${CA}.crl.srl" # CRL number file
EMPTY="${DIR}/crl/empty.crl"              # Just an empty CRL file

#SUBJ="/C=SE/ST=Blekinge/L=Karlskrona/O=EricssonAB/CN=TestRootCA"
#NUM="42"
SUBJ="/C=FI/ST=Habisko/L=Aabo/O=CompetitorOY/CN=FinnishRootCA"
NUM="01"

echo "Generate CRL test files (rev ${REV})."

# Clean working directory and set up a new clean infrastructure
#
echo "Prepare a clean structure in '${DIR}'."
rm -rf ${DIR} && mkdir -p ${DIR}/ca/${CA}/db ${DIR}/ca/private ${DIR}/crl
touch ${INDEX} ${EMPTY}
echo "${NUM}" | tee ${SERIAL} ${CRLNUM} > /dev/null

# Generate a self-signed root certificate and its corresponding private key (with no password)
#
echo "Prepare the self-signed certificate configuration."
echo "[ req ]
distinguished_name = req_distinguished_name

[ req_distinguished_name ]

[ req_ext ]
basicConstraints   = CA:TRUE,pathlen:0" > ${DIR}/ca/${CA}-ssl.conf

echo "Generate the self-signed certificate (with a private key)."
openssl req     \
    -x509       \
    -config     ${DIR}/ca/${CA}-ssl.conf \
    -extensions req_ext \
    -subj       ${SUBJ} \
    -sha256     \
    -newkey     ${ENCRYPTION} \
    -out        ${DIR}/ca/${CA}.crt \
    -keyout     ${DIR}/ca/private/${CA}.key \
    -nodes      \
    -days       ${DAYS}

# Create client/server certificates using the self-signed root certificate
#
echo "Create client certificate (with a private key)."
echo "[ reg ]
distinguished_name = req_distinguished_name
req_extensions     = req_ext

[ req_distinguished_name ]

[ req_ext ]
authorityKeyIdentifier = issuer,keyid
basicConstraints       = CA:FALSE
extendedKeyUsage       = clientAuth
keyUsage               = dataEncipherment,digitalSignature,keyEncipherment,nonRepudiation
subjectAltName         = @alt_names

[alt_names]
DNS.1                  = localhost" > ${DIR}/client-ssl.conf

openssl req \
    -new    \
    -subj   "/CN=${CN_CLIENT}" \
    -newkey ${ENCRYPTION} \
    -out    ${DIR}/client.csr \
    -keyout ${DIR}/client.key \
    -nodes

openssl x509        \
    -req            \
    -sha256         \
    -in             ${DIR}/client.csr \
    -CA             ${DIR}/ca/${CA}.crt \
    -CAkey          ${DIR}/ca/private/${CA}.key \
    -CAcreateserial \
    -CAserial       ${SERIAL} \
    -out            ${DIR}/client.crt \
    -extensions     req_ext \
    -extfile        ${DIR}/client-ssl.conf \
    -days           $((DAYS - 10))

echo "Create server certificate (with a private key)."
echo "[ reg ]
distinguished_name = req_distinguished_name
req_extensions     = req_ext

[ req_distinguished_name ]

[ req_ext ]
authorityKeyIdentifier = issuer,keyid
basicConstraints       = CA:FALSE
extendedKeyUsage       = serverAuth
keyUsage               = dataEncipherment,digitalSignature,keyEncipherment,nonRepudiation
subjectAltName         = @alt_names

[alt_names]
DNS.1                  = localhost" > ${DIR}/server-ssl.conf

openssl req \
    -new    \
    -subj   "/CN=${CN_SERVER}" \
    -newkey ${ENCRYPTION} \
    -out    ${DIR}/server.csr \
    -keyout ${DIR}/server.key \
    -nodes

openssl x509        \
    -req            \
    -sha256         \
    -in             ${DIR}/server.csr \
    -CA             ${DIR}/ca/${CA}.crt \
    -CAkey          ${DIR}/ca/private/${CA}.key \
    -CAcreateserial \
    -CAserial       ${SERIAL} \
    -out            ${DIR}/server.crt \
    -extensions     req_ext \
    -extfile        ${DIR}/server-ssl.conf \
    -days           $((DAYS - 10))

# Prepare the CRL configuration
#
echo "Prepare the CRL configuration."
echo "[ ca ]
default_ca             = root_ca

[ root_ca ]
certificate            = ${DIR}/ca/${CA}.crt
private_key            = ${DIR}/ca/private/${CA}.key
new_certs_dir          = ${DIR}/ca/certs
certs                  = ${DIR}/ca/certs
crl_dir                = ${DIR}/ca/crl
crl                    = ${DIR}/ca/crl/crl.pem
crlnumber              = ${DIR}/ca/${CA}/db/${CA}.crl.srl
database               = ${DIR}/ca/${CA}/db/${CA}.db          
serial                 = ${DIR}/ca/${CA}/db/${CA}.crt.srl     
x509_extensions        = usr_cert_extensions
default_crl_days       = ${DAYS}
default_days           = ${DAYS}
default_md             = sha256

[ usr_cert_extensions ]
authorityKeyIdentifier = keyid:always
basicConstraints       = CA:false
extendedKeyUsage       = clientAuth,serverAuth
keyUsage               = digitalSignature,keyEncipherment
subjectKeyIdentifier   = hash " > ${DIR}/ca/${CA}.conf

# Do a CRL with nothing revoked
#
echo "Create a CRL in the past where no certificates is revoked."
openssl ca   \
    -gencrl  \
    -config  ${DIR}/ca/${CA}.conf \
    -keyfile ${DIR}/ca/private/${CA}.key \
    -cert    ${DIR}/ca/${CA}.crt \
    -out     ${DIR}/crl/${CA}-revoked-nothing.crl \
    -crldays ${DAYS}

# Un/comment CLIENT and/or SERVER section below, depending on use case

# Revoke CLIENT certificate ...
#
#echo "Revoke client certificate '${DIR}/client.crt'."
#openssl ca  \
#    -revoke ${DIR}/client.crt \
#    -crl_reason certificateHold \
#    -config ${DIR}/ca/${CA}.conf

# ... and create a CRL with it
#
#echo "Create a CRL where the client certificate '${DIR}/client.crt' is revoked."
#openssl ca   \
#    -gencrl  \
#    -config  ${DIR}/ca/${CA}.conf \
#    -keyfile ${DIR}/ca/private/${CA}.key \
#    -cert    ${DIR}/ca/${CA}.crt \
#    -out     ${DIR}/crl/${CA}-revoked-certs-client.crl \
#    -crldays ${DAYS}

# ... or ...

# Revoke SERVER certificate ...
#
echo "Revoke server certificate '${DIR}/server.crt'."
openssl ca  \
    -revoke ${DIR}/server.crt \
    -crl_reason certificateHold \
    -config ${DIR}/ca/${CA}.conf

# ... and create a CRL with it
#
echo "Create a CRL where the server certificate '${DIR}/server.crt' is revoked."
openssl ca   \
    -gencrl  \
    -config  ${DIR}/ca/${CA}.conf \
    -keyfile ${DIR}/ca/private/${CA}.key \
    -cert    ${DIR}/ca/${CA}.crt \
    -out     ${DIR}/crl/${CA}-revoked-certs-server.crl \
    -crldays ${DAYS}


# Summary of files generated
#
echo "Root CA certificate in '${DIR}/ca/${CA}.crt'"
echo "Client certificate in '${DIR}/client.crt'"
echo "Server certificate in '${DIR}/server.crt'"
echo "Non-revoked certificate in '${DIR}/crl/${CA}-revoked-nothing-*.crl'"
echo "Revoked certificate in '${DIR}/crl/${CA}-revoked-certs-*.crl'"
echo "Empty CRL file in '${EMPTY}'."

echo "Done."
