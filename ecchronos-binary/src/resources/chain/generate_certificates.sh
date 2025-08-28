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

# Script to generate a complete certificate chain with:
# 1. Root CA
# 2. Intermediate CA
# 3. Server Certificate
# 4. Client Certificate

# Exit on error
set -e

# Create directory structure
echo "Creating directory structure..."
mkdir -p new_certs/{certs,private,newcerts,crl}
mkdir -p new_certs/intermediate/{certs,private,newcerts,crl,csr}
cd new_certs

# Initialize the certificate database files
touch index.txt
echo 1000 > serial
echo 1000 > crlnumber
touch intermediate/index.txt
echo 1000 > intermediate/serial
echo 1000 > intermediate/crlnumber

# Create OpenSSL configuration for Root CA
cat > openssl.cnf << 'EOL'
# OpenSSL root CA configuration file
[ ca ]
default_ca = CA_default
[ CA_default ]
# Directory and file locations
dir               = .
certs             = $dir/certs
crl_dir           = $dir/crl
new_certs_dir     = $dir/newcerts
database          = $dir/index.txt
serial            = $dir/serial
RANDFILE          = $dir/private/.rand
# The root key and root certificate
private_key       = $dir/private/ca.key
certificate       = $dir/certs/ca.cert
# For certificate revocation lists
crlnumber         = $dir/crlnumber
crl               = $dir/crl/ca.crl
crl_extensions    = crl_ext
default_crl_days  = 30
# SHA-1 is deprecated, so use SHA-2 instead
default_md        = sha256
name_opt          = ca_default
cert_opt          = ca_default
default_days      = 36500
preserve          = no
policy            = policy_strict
[ policy_strict ]
# The root CA should only sign intermediate certificates that match.
countryName             = optional
stateOrProvinceName     = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional
[ policy_loose ]
# Allow the intermediate CA to sign a more diverse range of certificates.
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional
[ req ]
default_bits        = 2048
distinguished_name  = req_distinguished_name
string_mask         = utf8only
default_md          = sha256
x509_extensions     = v3_ca
[ req_distinguished_name ]
commonName                      = Common Name
commonName_default              = Test Root CA
commonName_max                  = 64
[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
[ v3_intermediate_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
[ server_cert ]
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = @alt_names_server
[ client_cert ]
basicConstraints = CA:FALSE
nsCertType = client
nsComment = "OpenSSL Generated Client Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature
extendedKeyUsage = clientAuth
subjectAltName = @alt_names_client
[ alt_names_server ]
DNS.1 = localhost
DNS.2 = server.example.com
DNS.3 = server.local
[ alt_names_client ]
DNS.1 = client.example.com
DNS.2 = certified-client
URI.1 = spiffe://cluster.local/ns/example/sa/client
EOL

# Create OpenSSL configuration for Intermediate CA
cat > intermediate.cnf << 'EOL'
# OpenSSL intermediate CA configuration file
[ ca ]
default_ca = CA_default
[ CA_default ]
# Directory and file locations
dir               = .
certs             = $dir/intermediate/certs
crl_dir           = $dir/intermediate/crl
new_certs_dir     = $dir/intermediate/newcerts
database          = $dir/intermediate/index.txt
serial            = $dir/intermediate/serial
RANDFILE          = $dir/intermediate/private/.rand
# The root key and root certificate
private_key       = $dir/intermediate/private/intermediate.key
certificate       = $dir/intermediate/certs/intermediate.cert
# For certificate revocation lists
crlnumber         = $dir/intermediate/crlnumber
crl               = $dir/intermediate/crl/intermediate.crl
crl_extensions    = crl_ext
default_crl_days  = 30
# SHA-1 is deprecated, so use SHA-2 instead
default_md        = sha256
name_opt          = ca_default
cert_opt          = ca_default
default_days      = 365
preserve          = no
policy            = policy_loose
[ policy_loose ]
# Allow the intermediate CA to sign a more diverse range of certificates.
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional
[ req ]
default_bits        = 2048
distinguished_name  = req_distinguished_name
string_mask         = utf8only
default_md          = sha256
x509_extensions     = v3_intermediate_ca
[ req_distinguished_name ]
commonName                      = Common Name
commonName_default              = Test Intermediate CA
commonName_max                  = 64
[ v3_intermediate_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
[ server_cert ]
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = @alt_names_server
[ client_cert ]
basicConstraints = CA:FALSE
nsCertType = client
nsComment = "OpenSSL Generated Client Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature
extendedKeyUsage = clientAuth
subjectAltName = @alt_names_client
[ alt_names_server ]
DNS.1 = localhost
DNS.2 = server.example.com
DNS.3 = server.local
[ alt_names_client ]
DNS.1 = client.example.com
DNS.2 = certified-client
URI.1 = spiffe://cluster.local/ns/example/sa/client
EOL

# 1. Create Root CA
echo "Creating Root CA..."
openssl ecparam -genkey -name prime256v1 -out private/ca.key
chmod 400 private/ca.key
openssl req -config openssl.cnf -key private/ca.key -new -x509 -days 36500 -sha256 -extensions v3_ca -out certs/ca.cert \
    -subj "/CN=Test Internal Root CA"
chmod 444 certs/ca.cert

# 2. Create Intermediate CA
echo "Creating Intermediate CA..."
openssl ecparam -genkey -name prime256v1 -out intermediate/private/intermediate.key
chmod 400 intermediate/private/intermediate.key
openssl req -config intermediate.cnf -new -sha256 \
    -key intermediate/private/intermediate.key \
    -out intermediate/csr/intermediate.csr \
    -subj "/CN=Test Internal Intermediate CA"

# Sign the intermediate certificate with the root CA
openssl ca -config openssl.cnf -extensions v3_intermediate_ca \
    -days 36500 -notext -md sha256 \
    -in intermediate/csr/intermediate.csr \
    -out intermediate/certs/intermediate.cert \
    -batch
chmod 444 intermediate/certs/intermediate.cert

# Create the certificate chain file
cat intermediate/certs/intermediate.cert certs/ca.cert > intermediate/certs/ca-chain.cert
chmod 444 intermediate/certs/ca-chain.cert

# 3. Create Server Certificate
echo "Creating Server Certificate..."
openssl ecparam -genkey -name prime256v1 -out intermediate/private/server.key
chmod 400 intermediate/private/server.key
openssl req -config intermediate.cnf \
    -key intermediate/private/server.key \
    -new -sha256 -out intermediate/csr/server.csr \
    -subj "/CN=server.example.com"

# Sign the server certificate with the intermediate CA
openssl ca -config intermediate.cnf \
    -extensions server_cert -days 7 -notext -md sha256 \
    -in intermediate/csr/server.csr \
    -out intermediate/certs/server.cert \
    -batch
chmod 444 intermediate/certs/server.cert

# 4. Create Client Certificate
echo "Creating Client Certificate..."
openssl ecparam -genkey -name prime256v1 -out intermediate/private/client.key
chmod 400 intermediate/private/client.key
openssl req -config intermediate.cnf \
    -key intermediate/private/client.key \
    -new -sha256 -out intermediate/csr/client.csr \
    -subj "/CN=client.example.com"

# Sign the client certificate with the intermediate CA
openssl ca -config intermediate.cnf \
    -extensions client_cert -days 7 -notext -md sha256 \
    -in intermediate/csr/client.csr \
    -out intermediate/certs/client.cert \
    -batch
chmod 444 intermediate/certs/client.cert

# Verify the certificates
echo "Verifying certificates..."
openssl verify -CAfile certs/ca.cert intermediate/certs/intermediate.cert
openssl verify -CAfile intermediate/certs/ca-chain.cert intermediate/certs/server.cert
openssl verify -CAfile intermediate/certs/ca-chain.cert intermediate/certs/client.cert

# Create README file
cat > README.md << 'EOL'
# Certificate Chain
This directory contains a complete certificate chain with:
1. Root CA (`certs/ca.cert`)
2. Intermediate CA (`intermediate/certs/intermediate.cert`)
3. Server Certificate (`intermediate/certs/server.cert`)
4. Client Certificate (`intermediate/certs/client.cert`)
## Certificate Chain Structure
```
Root CA
  |
  +-- Intermediate CA
       |
       +-- Server Certificate
       |
       +-- Client Certificate
```
## Files
### Root CA
- Certificate: `certs/ca.cert`
- Private Key: `private/ca.key`
### Intermediate CA
- Certificate: `intermediate/certs/intermediate.cert`
- Private Key: `intermediate/private/intermediate.key`
- CA Chain (Intermediate + Root): `intermediate/certs/ca-chain.cert`
### Server Certificate
- Certificate: `intermediate/certs/server.cert`
- Private Key: `intermediate/private/server.key`
### Client Certificate
- Certificate: `intermediate/certs/client.cert`
- Private Key: `intermediate/private/client.key`
## Certificate Properties
- **Root CA**: Valid for 100 years, uses ECDSA with P-256 curve
- **Intermediate CA**: Valid for 100 years, uses ECDSA with P-256 curve
- **Server Certificate**: Valid for 7 days, uses ECDSA with P-256 curve, includes server authentication extensions
- **Client Certificate**: Valid for 7 days, uses ECDSA with P-256 curve, includes client authentication extensions
## Verification
To verify the certificate chain:
```bash
openssl verify -CAfile certs/ca.cert intermediate/certs/intermediate.cert
openssl verify -CAfile intermediate/certs/ca-chain.cert intermediate/certs/server.cert
openssl verify -CAfile intermediate/certs/ca-chain.cert intermediate/certs/client.cert
```
## Misc notes
If keys are generated in an old format (e.g. the file contains 'EC PARAMETERS' etc), it needs to be converted
into a newer PKCS format. Example:
```bash
openssl pkcs8 -topk8 -nocrypt -in intermediate/private/client.key -out intermediate/private/client_pkcs8.key
```
EOL

echo "Certificate generation complete!"
echo "All certificates are in the 'new_certs' directory."