CREATE SERVER AND CLIENT KEYSTORE/TRUSTSTORE FILES FOR TESTING:

(create working directory)
$ mkdir my_test_files
$ cd my_test_files

(remove any old files)
$ rm -rf certs1 certs2 keystore_s.jks keystore_c.jks truststore_s.jks truststore_c.jks

(generate a new set of certificates and CRLs. if another set is needed rerun with diretory 'certs2')
$ ./gencrl.sh certs1

(create keystore/truststore for server side)
$ openssl pkcs12 -export -in certs1/server.crt -inkey certs1/server.key -name "server-cert" -out keystore_s.p12 -password pass:123456
$ keytool -importkeystore -srckeystore keystore_s.p12 -srcstoretype PKCS12 -destkeystore keystore_s.jks -deststoretype JKS -noprompt -srcstorepass 123456 -deststorepass 123456
$ keytool -import -trustcacerts -alias client-cert -file certs1/client.crt -keystore truststore_s.jks -noprompt -storepass 123456

(create keystore/truststore for client side)
$ openssl pkcs12 -export -in certs1/client.crt -inkey certs1/client.key -name "client-cert" -out keystore_c.p12 -password pass:123456
$ keytool -importkeystore -srckeystore keystore_c.p12 -srcstoretype PKCS12 -destkeystore keystore_c.jks -deststoretype JKS -noprompt -srcstorepass 123456 -deststorepass 123456
$ keytool -import -trustcacerts -alias server-cert -file certs1/server.crt -keystore truststore_c.jks -noprompt -storepass 123456

(remove intermediate files)
$ rm keystore_s.p12 keystore_c.p12

(add the issuing root ca certificate to the server; needed to authenticate the clients certificate at the server)
$ keytool -import -trustcacerts -alias root-ca-cert -file certs1/ca/root-ca.crt -keystore truststore_s.jks -noprompt -storepass 123456

(add the same on the client side)
$ keytool -import -trustcacerts -alias root-ca-cert -file certs1/ca/root-ca.crt -keystore truststore_c.jks -noprompt -storepass 123456

(validate that all is in place in the keystores/truststores, server files)
$ echo ">>>>>>>>>>>>>>>>> SERVER KEYSTORE >>>>>>>>>>>>>>>>>" && \
  keytool -list -v -keystore keystore_s.jks -storepass 123456 2> /dev/null | grep "Alias" && \
  echo ">>>>>>>>>>>>>>>> SERVER TRUSTSTORE >>>>>>>>>>>>>>>>" && \
  keytool -list -v -keystore truststore_s.jks -storepass 123456 | grep "Alias" && \
(... client files)
$ echo && \
  echo ">>>>>>>>>>>>>>>>> CLIENT KEYSTORE >>>>>>>>>>>>>>>>>" && \
  keytool -list -v -keystore keystore_c.jks -storepass 123456 2> /dev/null | grep "Alias" && \
  echo ">>>>>>>>>>>>>>>> CLIENT TRUSTSTORE >>>>>>>>>>>>>>>>" && \
  keytool -list -v -keystore truststore_c.jks -storepass 123456 | grep "Alias"


CONFIGURE ECCHRONOS:

(edit 'security.yml')
...
cql:
  credentials:
    enabled: false
    username: cassandra
    password: cassandra
  tls:
    enabled: true
    keystore: /.../keystore_c.jks
    keystore_password: 123456
    truststore: /.../truststore_c.jks
    truststore_password: 123456
    certificate:
    certificate_private_key:
    trust_certificate:
    protocol: TLSv1.2
    algorithm:
    store_type: JKS
    cipher_suites:
    require_endpoint_verification: false
    crl:
      enabled: true
      path: /.../entries.crl
      strict: true
      attempts: 5
      interval: 300


jmx:
  credentials:
    enabled: true
    username: cassandra
    password: cassandra
  tls:
    enabled: true
    keystore: /.../keystore_c.jks
    keystore_password: 123456
    truststore: /.../truststore_c.jks
    truststore_password: 123456
    protocol: TLSv1.2
    cipher_suites:


(edit 'ecc.yml')
...
host: localhost
    port: 7100
...
  interval:
    time: 5
    unit: minutes
...
  initial_delay:
    time: 5
    unit: minutes
...


CONFIGURING CASSANDRA:

(JMX, see ./conf/cassandra-env.sh in cassandra)
$ echo $JVM_OPTS 
-Xlog:gc=info,heap=trace,age=debug,safepoint=info,promotion=trace:file=/.../.ccm/test/node1/logs/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10240
-Xms8192M
-Xmx8192M
-XX:CompileCommandFile=/hotspot_compiler -javaagent:/lib/jamm-0.4.0.jar
-Dcassandra.jmx.local.port=7100
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.password.file=/.../.ccm/test/node1/jmxremote.password
-Djava.library.path=/lib/sigar-bin -XX:CompileCommandFile=/hotspot_compiler -javaagent:/lib/jamm-0.4.0.jar
-Dcassandra.jmx.local.port=7100
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.password.file=/.../.ccm/test/node1/jmxremote.password
-Djava.library.path=/lib/sigar-bin

$ cat ~/.ccm/test/node1/jmxremote.password 
cassandra cassandra
$ ccm stop
$ export LOCAL_JMX="no"
$ ccm start

(CQL, edit cassandra.yml on all nodes ecchrons will connect to)
...
client_encryption_options:
   enabled: true
   optional: false
   keystore: .../keystore_s.jks
   keystore_password: 123456
   require_client_auth: true
   truststore: ...truststore_s.jks
   truststore_password: 123456
...


TRYING OUT SCENARIOS:

(set CRL to nothing revoked)
$ certs1/crl/root-ca-revoked-nothing.crl > /.../entries.crl

(set CRL to server cert revoked)
$ cp certs1/crl/root-ca-revoked-certs-server.crl > /.../entries.crl

...

