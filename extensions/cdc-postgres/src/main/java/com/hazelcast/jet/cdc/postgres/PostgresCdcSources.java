/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSource;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSource;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.cdc.postgres.impl.PostgresSequenceExtractor;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

/**
 * Contains factory methods for creating change data capture sources
 * based on PostgreSQL databases.
 *
 * @since 4.2
 */
@EvolvingApi
public final class PostgresCdcSources {

    private PostgresCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a PostgreSQL database
     * to Hazelcast Jet.
     * <p>
     * Behaviour of the source on connection disruptions to the database is
     * configurable and is governed by the {@code setReconnectBehaviour(String)}
     * setting (as far as the underlying Debezium connector cooperates, read
     * further for details).
     * <p>
     * The default reconnect behaviour is <em>FAIL</em>, which threats any
     * connection failure as an unrecoverable problem and produces the failure
     * of the source and the entire job. (How Jet handles job failures and what
     * ways there are for recovering from them, is a generic issue not discussed
     * here.)
     * <p>
     * The other two behaviour options, <em>RECONNECT</em> and
     * <em>CLEAR_STATE_AND_RECONNECT</em>, instruct the source to try to
     * automatically recover from any connection failure by restarting
     * the whole source. The two types of behaviour differ from each-other in
     * how exactly they handle the restart, if they preserve the current state
     * of the source or if they reset it. If the state is kept, then
     * snapshotting should not be repeated and streaming the WAL should resume
     * at the position where it left off. If the state is reset, then the source
     * will behave as if it were its initial start, so will do a snapshot and
     * will start trailing the WAL where it syncs with the snapshot's end.
     * <p>
     * One caveat of the restart process is that it can work correctly only as
     * long as the Postgres replication slot, which it's based on, has
     * <em>not</em> lost data. When the Postgres database cluster experiences
     * failures and the source needs to be connected to a different database
     * instance, manual intervention from an administrator might become
     * necessary to ensure that replication slot has been re-created properly,
     * without data loss.
     * <p>
     * Depending on the lifecycle phase the source is in, there are some
     * discrepancies and peculiarities in this behaviour. There are also further
     * settings for influencing it. See what follows for details.
     * <p>
     * On the <em>initial start</em> of the connector, if the reconnect
     * behaviour is set to <em>FAIL</em> and the database is not immediately
     * reachable, the source will fail. Otherwise, it will try to reconnect
     * until it succeeds. How much it will wait between two successive reconnect
     * attempts can be configured via the {@code setReconnectIntervalMs(long)}
     * setting.
     * <p>
     * If the connection to the database fails, either during the
     * <em>snapshotting</em>, or the <em>WAL trailing</em> phase, then the
     * connector notices the failure only after a very long delay
     * (have measured it to 150 seconds and have not found a way to configure
     * it). If the connection recovers in the meantime, then it will resume
     * operations like nothing had happened. If the connection goes down due to
     * the database being shut down cleanly, the connector can detect it and
     * react immediately, but if the outage is purely at the network level,
     * then, more often than not, the above mentioned long delay needs to be
     * waited out.
     *
     * @param name name of this source, needs to be unique, will be passed to
     *             the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder postgres(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a PostgreSQL database to Hazelcast Jet.
     */
    public static final class Builder {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.dbname")
                .inclusive("database.sslkey", "database.sslpassword")
                .exclusive("schema.whitelist", "schema.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique, will be passed to
         *             the underlying Kafka Connect source
         */
        private Builder(String name) {
            Objects.requireNonNull(name, "name");

            config = new DebeziumConfig(name, "io.debezium.connector.postgresql.PostgresConnector");
            config.setProperty(CdcSource.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, PostgresSequenceExtractor.class.getName());
            config.setProperty(ChangeRecordCdcSource.DB_SPECIFIC_EXTRA_FIELDS_PROPERTY, "schema");
            config.setProperty("database.server.name", UuidUtil.newUnsecureUuidString());
            config.setProperty("snapshot.mode", "exported");
        }

        /**
         * IP address or hostname of the database server, has to be specified.
         */
        @Nonnull
        public Builder setDatabaseAddress(@Nonnull String address) {
            config.setProperty("database.hostname", address);
            return this;
        }

        /**
         * Optional port number of the database server, if unspecified defaults
         * to the database specific default port (5432).
         */
        @Nonnull
        public Builder setDatabasePort(int port) {
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user for connecting to the database server. Has to be
         * specified.
         */
        @Nonnull
        public Builder setDatabaseUser(@Nonnull String user) {
            config.setProperty("database.user", user);
            return this;
        }

        /**
         * Database user password for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder setDatabasePassword(@Nonnull String password) {
            config.setProperty("database.password", password);
            return this;
        }

        /**
         * The name of the PostgreSQL database from which to stream the changes.
         * Has to be set.
         * <p>
         * Currently this source is not capable of monitoring multiple
         * databases, only multiple schemas and/or tables. See white- and
         * black-listing configuration options for those.
         */
        @Nonnull
        public Builder setDatabaseName(@Nonnull String dbName) {
            config.setProperty("database.dbname", dbName);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be monitored
         * ("schema" is used here to denote logical groups of tables). Any
         * schema name not included in the whitelist will be excluded from
         * monitoring. By default all non-system schemas will be monitored. May
         * not be used with
         * {@link #setSchemaBlacklist(String...) schema blacklist}.
         */
        @Nonnull
        public Builder setSchemaWhitelist(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.whitelist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be excluded
         * from monitoring ("schema" is used here to denote logical groups of
         * tables). Any schema name not included in the blacklist will be
         * monitored, with the exception of system schemas. May not be used with
         * {@link #setSchemaWhitelist(String...) schema whitelist}.
         */
        @Nonnull
        public Builder setSchemaBlacklist(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.blacklist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>schemaName.tableName</em>. By default the connector will
         * monitor every non-system table in each monitored database. May not be
         * used with {@link #setTableBlacklist(String...) table blacklist}.
         */
        @Nonnull
        public Builder setTableWhitelist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.whitelist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any table not
         * included in the blacklist will be monitored. Each identifier is of
         * the form <em>schemaName.tableName</em>. May not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        @Nonnull
        public Builder setTableBlacklist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.blacklist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match the fully-qualified names of
         * columns that should be excluded from change event message values.
         * Fully-qualified names for columns are of the form
         * <em>schemaName.tableName.columnName</em>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.blacklist", columnNameRegExps);
            return this;
        }

        /**
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding.html">
         * Postgres logical decoding plug-in</a> installed on the server.
         * Supported values are <em>decoderbufs</em>, <em>wal2json</em>,
         * <em>wal2json_rds</em>, <em>wal2json_streaming</em>,
         * <em>wal2json_rds_streaming</em> and <em>pgoutput</em>.
         * <p>
         * If not explicitly set, the property defaults to <em>decoderbufs</em>.
         * <p>
         * When the processed transactions are very large it is possible that
         * the JSON batch event with all changes in the transaction will not fit
         * into the hard-coded memory buffer of size 1 GB. In such cases it is
         * possible to switch to so-called streaming mode when every change in
         * transactions is sent as a separate message from PostgreSQL.
         */
        @Nonnull
        public Builder setLogicalDecodingPlugIn(@Nonnull String pluginName) {
            config.setProperty("plugin.name", pluginName);
            return this;
        }

        /**
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding-explanation.html">
         * Postgres logical decoding slot</a> (also called "replication slot")
         * created for streaming changes from a plug-in and database instance.
         * <p>
         * Values must conform to Postgres replication slot naming rules which
         * state: "Each replication slot has a name, which can contain
         * lower-case letters, numbers, and the underscore character."
         * <p>
         * Replication slots have to have an identifier that is unique across
         * all databases in a PostgreSQL cluster.
         * <p>
         * If not explicitly set, the property defaults to <em>debezium</em>.
         */
        @Nonnull
        public Builder setReplicationSlotName(@Nonnull String slotName) {
            config.setProperty("slot.name", slotName);
            return this;
        }

        /**
         * Whether or not to drop the logical replication slot when the
         * connector disconnects cleanly.
         * <p>
         * Defaults to <em>false</em>
         * <p>
         * Should only be set to <em>true</em> in testing or development
         * environments. Dropping the slot allows WAL segments to be discarded
         * by the database, so it may happen that after a restart the connector
         * cannot resume from the WAL position where it left off before.
         */
        @Nonnull
        public Builder setReplicationSlotDropOnStop(boolean dropOnStop) {
            config.setProperty("slot.drop.on.stop", dropOnStop);
            return this;
        }

        /**
         * The name of the <a href="https://www.postgresql.org/docs/10/logical-replication-publication.html">
         * Postgres publication</a> that will be used for CDC purposes.
         * <p>
         * If the publication does not exist when this source starts up, then
         * the source will create it (note: the database user of the source must
         * have superuser permissions to be able to do so). If created this way
         * the publication will include all tables and the source itself must
         * filter the data based on its white-/blacklist configs. This is not
         * efficient because the database will still send all data to the
         * connector, before filtering is applied.
         * <p>
         * It's best to use a pre-defined publication (via the
         * <code>CREATE PUBLICATION</code> SQL command, specified via its name.
         * <p>
         * If not explicitly set, the property defaults to <em>dbz_publication</em>.
         */
        @Nonnull
        public Builder setPublicationName(@Nonnull String publicationName) {
            config.setProperty("publication.name", publicationName);
            return this;
        }

        /**
         * Specifies whether to use an encrypted connection to the database. The
         * default is <em>disable</em>, and specifies to use an unencrypted
         * connection.
         * <p>
         * The <em>require</em> option establishes an encrypted connection but
         * will fail if one cannot be made for any reason.
         * <p>
         * The <em>verify_ca</em> option behaves like <em>require</em> but
         * additionally it verifies the server TLS certificate against the
         * configured Certificate Authority (CA) certificates and will fail if
         * it doesn’t match any valid CA certificates.
         * <p>
         * The <em>verify-full</em> option behaves like <em>verify_ca</em> but
         * additionally verifies that the server certificate matches the host
         * of the remote connection.
         */
        @Nonnull
        public Builder setSslMode(@Nonnull String mode) {
            config.setProperty("database.sslmode", mode);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL Certificate for
         * the database client.
         */
        @Nonnull
        public Builder setSslCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslcert", file);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL private key of
         * the database client.
         */
        @Nonnull
        public Builder setSslKeyFile(@Nonnull String file) {
            config.setProperty("database.sslkey", file);
            return this;
        }

        /**
         * Specifies the password to be used to access the SSL key file, if
         * specified.
         * <p>
         * Mandatory if key file specified.
         */
        @Nonnull
        public Builder setSslKeyFilePassword(@Nonnull String password) {
            config.setProperty("database.sslpassword", password);
            return this;
        }

        /**
         * Specifies the file containing containing SSL certificate authority
         * (CA) certificate(s).
         */
        @Nonnull
        public Builder setSslRootCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslrootcert", file);
            return this;
        }

        /**
         * Interval in milliseconds after which to retry connecting, if previous
         * attempts have failed. Defaults to
         * {@value CdcSource#DEFAULT_RECONNECT_INTERVAL_MS} milliseconds.
         */
        @Nonnull
        public Builder setReconnectIntervalMs(long intervalMs) {
            config.setProperty(CdcSource.RECONNECT_INTERVAL_MS, intervalMs);
            config.setProperty("slot.retry.delay.ms", intervalMs);
            return this;
        }

        /**
         * Specifies how the connector should behave when it looses connection
         * to the backing database.
         * <p>
         * Possible values are:
         * <ul>
         *     <li><em>fail</em>: will cause the whole job to fail</li>
         *     <li><em>clear_state_and_reconnect</em>: will reconnect to
         *      database, but will clear all internal state first, thus behaving
         *      as if it would be connecting the first time (for example
         *      snapshotting will be repeated)</li>
         *     <li><em>reconnect</em>: will reconnect as is, in the same state
         *      as it was at the moment of the disconnect </li>
         * </ul>
         */
        @Nonnull
        public Builder setReconnectBehaviour(String behaviour) {
            config.setProperty(CdcSource.RECONNECT_BEHAVIOUR_PROPERTY, behaviour);
            return this;
        }

        /**
         * Can be used to set any property not explicitly covered by other
         * methods or to override properties we have hidden.
         */
        @Nonnull
        public Builder setCustomProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns the source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            Properties properties = config.toProperties();
            RULES.check(properties);
            return ChangeRecordCdcSource.fromProperties(properties);
        }

    }
}
