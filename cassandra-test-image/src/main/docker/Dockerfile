FROM cassandra:@it.cassandra.version@

COPY ecc-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/ecc-entrypoint.sh /ecc-entrypoint.sh

COPY create_keyspaces.cql /etc/cassandra/
COPY users.cql /etc/cassandra/
COPY setup_db.sh /etc/cassandra/

ENTRYPOINT ["ecc-entrypoint.sh"]
EXPOSE 7000 7001 7199 9042
