/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;
import org.apache.cassandra.service.CassandraDaemon;

import static org.awaitility.Awaitility.await;

/**
 * Singleton for creating a CassandraDaemon used for running test cases in multiple files.
 */
public class CassandraDaemonForECChronos implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraDaemonForECChronos.class);

    private static CassandraDaemonForECChronos myCassandraDaemonForTest;
    private static CassandraDaemon myCassandraDaemon;
    private static File myTempDir;
    private static volatile int myRpcPort = -1;
    private static volatile int myStoragePort = -1;
    private static volatile int mySslStoragePort = -1;
    private static volatile int myNativePort = -1;
    private static volatile int myJmxPort = -1;
    private static AtomicBoolean isStopped = new AtomicBoolean(false);
    private Thread myCassandraThread;

    private CassandraDaemonForECChronos() throws IOException
    {
        synchronized (CassandraDaemonForECChronos.class)
        {
            if (myCassandraDaemon == null)
            {
                randomizePorts();
                activate();
                myCassandraDaemon = new CassandraDaemon(true);
                myCassandraThread = new Thread(this);
                myCassandraThread.setDaemon(true);
                myCassandraThread.start();
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    if (myCassandraThread != null)
                    {
                        myCassandraThread.join();
                    }
                    myCassandraDaemon.deactivate();
                    if (myCassandraThread != null)
                    {
                        myCassandraThread.interrupt();
                    }

                    try
                    {
                        Thread.sleep(2000);
                    }
                    catch (InterruptedException e)
                    {
                        // Ignored
                    }

                    if (myTempDir != null && myTempDir.exists())
                    {
                        try
                        {
                            FileUtils.deleteDirectory(myTempDir);
                        }
                        catch (IOException e)
                        {
                            LOG.error("Error deleting temp files : " + e.getMessage());
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    LOG.error("Error joining cassandra thread : " + e.getMessage());
                }
            }
        });
    }

    /**
     * Creates new CassandraDaemon instance.
     *
     * @return CassandraDaemonForTest
     * @throws IOException
     */
    public static CassandraDaemonForECChronos getInstance() throws IOException
    {
        await().until(() -> !isStopped.get());

        if (myCassandraDaemonForTest == null)
        {
            myCassandraDaemonForTest = new CassandraDaemonForECChronos();
        }

        synchronized (CassandraDaemonForECChronos.class)
        {
            if (!myCassandraDaemon.setupCompleted() && !myCassandraDaemon.isNativeTransportRunning())
            {
                myCassandraDaemon.activate();

                await().atMost(10, TimeUnit.SECONDS).until(() -> myCassandraDaemon.isNativeTransportRunning());
            }
            else if (!myCassandraDaemon.isNativeTransportRunning())
            {
                myCassandraDaemon.start();

                await().atMost(10, TimeUnit.SECONDS).until(() -> myCassandraDaemon.isNativeTransportRunning());
            }
        }

        return myCassandraDaemonForTest;
    }

    /**
     * Stops the CassandraDaemon.
     *
     * @throws IOException
     */
    public void stop() throws IOException
    {
        synchronized (CassandraDaemonForECChronos.class)
        {
            isStopped.getAndSet(true);
            myCassandraDaemon.stop();
        }
    }

    /**
     * Stops the native Server
     *
     * @throws IOException
     */
    public void stopNative() throws IOException
    {
        synchronized (CassandraDaemonForECChronos.class)
        {
            isStopped.getAndSet(true);
            myCassandraDaemon.stopNativeTransport();
        }
    }

    /**
     * Set the CassandraDaemon "restartable".
     *
     * @throws IOException
     */
    public void restart() throws IOException
    {
        synchronized (CassandraDaemonForECChronos.class)
        {
            isStopped.getAndSet(false);
            if (!myCassandraDaemon.isNativeTransportRunning())
            {
                if (!myCassandraDaemon.setupCompleted())
                {
                    myCassandraDaemon.init(null);
                }
                myCassandraDaemon.start();
            }

            await().atMost(1, TimeUnit.SECONDS).until(() -> myCassandraDaemon.isNativeTransportRunning());
        }
    }

    /**
     * Get the random created Cassandra port.
     *
     * @return the port
     */
    public int getPort()
    {
        return myNativePort;
    }

    /**
     * Get the random created JMX server port.
     *
     * @return the port
     */
    public int getJMXPort()
    {
        return myJmxPort;
    }

    /**
     * Is SSL enabled on the server
     *
     */
    public boolean isSSLEnabled()
    {
        return DatabaseDescriptor.getClientEncryptionOptions().enabled;
    }

    /**
     * Is Server authentication enabled
     *
     */
    public boolean isAuthenticationEnabled()
    {
        return DatabaseDescriptor.getAuthenticator().requireAuthentication();
    }

    /**
     * Creates a Cluster object that can be used to connect to this server This function will return a client which uses the same SSL parameters which
     * the server uses.
     * <p>
     * Don't forget to close the returned cluster object when done.
     *
     * @return Cluster object
     */
    public Cluster getCluster()
    {
        Builder builder = Cluster.builder();
        builder.addContactPoint(DatabaseDescriptor.getListenAddress().getHostAddress()).withPort(myNativePort).withCredentials("cassandra", "cassandra").withReconnectionPolicy(new ConstantReconnectionPolicy(100));
        if (isSSLEnabled())
        {
            builder.withSSL(generateSslOptions());
        }

        return builder.build();
    }

    private void randomizePorts()
    {
        myRpcPort = randomAvailablePort();
        myStoragePort = randomAvailablePort();
        mySslStoragePort = randomAvailablePort();
        myNativePort = randomAvailablePort();
        myJmxPort = randomAvailablePort();
    }

    private int randomAvailablePort()
    {
        int port = -1;
        while (port < 0)
        {
            port = (new Random().nextInt(16300) + 49200);
            if (myRpcPort == port
                    || myStoragePort == port
                    || mySslStoragePort == port
                    || myNativePort == port
                    || myJmxPort == port)
            {
                port = -1;
            }
            else
            {
                try (ServerSocket socket = new ServerSocket(port))
                {
                    break;
                }
                catch (IOException e)
                {
                    port = -1;
                }
            }
        }
        return port;
    }

    /**
     * Generate an instance of SSLOptions from the configuration.
     *
     * @return an instance of SSLOptions for the Cassandra ClusterBuilder, or null
     */
    @SuppressWarnings (
            { "squid:S2440" })
    private SSLOptions generateSslOptions()
    {
        KeyManager[] keyManagers = getKeyManagers();
        if (keyManagers == null)
        {
            LOG.error("Failed to initialize keymanager - Trying SSL/TLS without it");
        }

        TrustManager[] trustManagers = getDummyTrustManagers(); // get Trustmanager which accepts all keys

        SSLOptions sslOptions = null;
        try
        {
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagers, trustManagers, new SecureRandom());

            String[] cipherSuites = DatabaseDescriptor.getClientEncryptionOptions().cipher_suites;
            sslOptions = RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).withCipherSuites(cipherSuites).build();
        }
        catch (NoSuchAlgorithmException e)
        {
            LOG.error("Environment does support {} - Proceeding without SSL/TLS", DatabaseDescriptor.getClientEncryptionOptions().protocol);
        }
        catch (KeyManagementException e)
        {
            LOG.error("Failed to initialize SSL/TLS key management - Proceeding without SSL/TLS");
        }

        return sslOptions;
    }

    /**
     * Create a key manager which wraps the keystore defined by the same parameters which the server uses
     *
     * @return An array with one KeyManager.
     */
    private KeyManager[] getKeyManagers()
    {
        ClientEncryptionOptions encryptionOptions = DatabaseDescriptor.getClientEncryptionOptions();
        KeyStore keystore = null;
        try (FileInputStream keystoreFile = new FileInputStream(encryptionOptions.keystore))
        {
            keystore = KeyStore.getInstance(encryptionOptions.store_type);
            keystore.load(keystoreFile, encryptionOptions.keystore_password.toCharArray());
        }
        catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e)
        {
            LOG.error("Unable to load keystore file {} : {}", encryptionOptions.keystore, e);
            return null;
        }

        KeyManagerFactory keyManagerFactory;
        try
        {
            keyManagerFactory = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keystore, encryptionOptions.keystore_password.toCharArray());
        }
        catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e)
        {
            LOG.error("Failed to initialize keystore factory with key material", e);
            return null;
        }

        return keyManagerFactory.getKeyManagers();
    }

    /**
     * Dummy Trust manager used when no truststore is supplied.
     *
     * @return an array containing one TrustManager
     */
    private TrustManager[] getDummyTrustManagers()
    {
        return new TrustManager[]
                { new DummyTrustManager() };
    }

    private static class DummyTrustManager implements X509TrustManager
    {
        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType)
        {
            // Dummy class which should not do anything
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType)
        {
            // Dummy class which should not do anything
        }
    }

    /**
     * Create the Cassandra configuration files to be used."
     *
     * @throws IOException
     */
    private void activate() throws IOException
    {
        myTempDir = Files.createTempDir();
        myTempDir.deleteOnExit();

        InputStream inStream = CassandraDaemonForECChronos.class.getClassLoader().getResourceAsStream("cassandra.yaml");
        String content = readStream(inStream);

        Path outPath = Paths.get(myTempDir.getPath() + "/cassandra.yaml");
        content = content.replaceAll("###tmp###", myTempDir.getPath().replace("\\", "\\\\"));
        content = content.replaceAll("###rpc_port###", String.valueOf(myRpcPort));
        content = content.replaceAll("###storage_port###", String.valueOf(myStoragePort));
        content = content.replaceAll("###ssl_storage_port###", String.valueOf(mySslStoragePort));
        content = content.replaceAll("###native_transport_port###", String.valueOf(myNativePort));
        java.nio.file.Files.write(outPath, content.getBytes(StandardCharsets.UTF_8));

        System.setProperty("cassandra.config", outPath.toUri().toURL().toExternalForm());

        System.setProperty("cassandra.jmx.local.port", String.valueOf(myJmxPort));

        inStream = CassandraDaemonForECChronos.class.getClassLoader().getResourceAsStream("cassandra-rackdc.properties");
        content = readStream(inStream);

        outPath = Paths.get(myTempDir.getPath() + "/cassandra-rackdc.properties");
        java.nio.file.Files.write(outPath, content.getBytes(StandardCharsets.UTF_8));

        System.setProperty("cassandra-rackdc.properties", outPath.toUri().toURL().toExternalForm());

        System.setProperty("cassandra-foreground", "true");

        // add for speeding up test; this option disables durable_writes
        System.setProperty("cassandra.unsafesystem", "true");
    }

    /**
     * Reads the default Cassandra configuration info from file.
     *
     * @param inputStream
     * @return the content as a String
     * @throws IOException
     */
    private static String readStream(InputStream inputStream) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length = 0;
        while ((length = inputStream.read(buffer)) != -1)
        {
            out.write(buffer, 0, length);
        }
        return new String(out.toByteArray(), "UTF-8");
    }

    @Override
    public void run()
    {
        try
        {
            getInstance();
        }
        catch (IOException e)
        {
            LOG.error("Error starting cassandra : " + e.getMessage());
        }
    }
}
