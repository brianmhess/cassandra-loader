/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.KeyStoreException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;


public class CassandraClusterParameters {
    private String host = null;
    private int port = null;
    private String username = null;
    private String password = null;
    private String truststorePath = null;
    private String truststorePwd = null;
    private String keystorePath = null;
    private String keystorePwd = null;

    private PoolingOptionsParameters poolingOptionsParameters = new PoolingOptionsParameters();

    public CassandraCluster() { }

    public boolean parseArgs(Map<String,String> amap) {
        String tkey;

        host = amap.remove("-host");
        if (null == host) { // host is required
            System.err.println("Must provide a host");
            return false;
        }
        if (null != (tkey = amap.remove("-port")))          port = Integer.parseInt(tkey);
        if (null != (tkey = amap.remove("-user")))          username = tkey;
        if (null != (tkey = amap.remove("-pw")))            password = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-path"))) truststorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-truststore-pw")))  truststorePwd = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-path")))   keystorePath = tkey;
        if (null != (tkey = amap.remove("-ssl-keystore-pw")))    keystorePwd = tkey;

        poolingOptionsParameters.parseArgs(amap);

        return validateArgs();
    }

    public boolean validateArgs() {
        if ((null == username) && (null != password)) {
            System.err.println("If you supply the password, you must supply the username");
            return false;
        }
        if ((null != username) && (null == password)) {
            System.err.println("If you supply the username, you must supply the password");
            return false;
        }
        if ((null == truststorePath) && (null != truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-pw, you must supply the ssl-truststore-path");
            return false;
        }
        if ((null != truststorePath) && (null == truststorePwd)) {
            System.err.println("If you supply the ssl-truststore-path, you must supply the ssl-truststore-pw");
            return false;
        }
        if ((null == keystorePath) && (null != keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-pw, you must supply the ssl-keystore-path");
            return false;
        }
        if ((null != keystorePath) && (null == keystorePwd)) {
            System.err.println("If you supply the ssl-keystore-path, you must supply the ssl-keystore-pw");
            return false;
        }
        File tfile = null;
        if (null != truststorePath) {
            tfile = new File(truststorePath);
            if (!tfile.isFile()) {
                System.err.println("truststore file must be a file");
                return false;
            }
        }
        if (null != keystorePath) {
            tfile = new File(keystorePath);
            if (!tfile.isFile()) {
                System.err.println("keystore file must be a file");
                return false;
            }
        }
        if (!poolingOptionsParameters.validateArgs())
            return false;

        return true;
    }

    public String usage() {
        StringBuilder usage = new StringBuilder(" Cassandra Connection Parameters:\n");
        usage.append("  -host <ipaddress>                  Connection point [REQUIRED]\n");
        usage.append("  -port <portNumber>                 CQL Port Number [9042]\n");
        usage.append("  -user <username>                   Cassandra username [none]\n");
        usage.append("  -pw <password>                     Password for user [none]\n");
        usage.append("  -ssl-truststore-path <path>        Path to SSL truststore [none]\n");
        usage.append("  -ssl-truststore-pw <pwd>           Password for SSL truststore [none]\n");
        usage.append("  -ssl-keystore-path <path>          Path to SSL keystore [none]\n");
        usage.append("  -ssl-keystore-pw <pwd>             Password for SSL keystore [none]\n");
        
        usage.append(poolingOptionsParameters.usage());
        return usage.toString();
    }

    public boolean setClusterOptions(Cluster.Builder clusterBuilder) {
        PoolingOptions po - new PoolingOptions();
        poolingOptionsParameters.setPoolingOptions(po);

        clusterBuilder = clusterBuilder.addContactPoint(host);
        clusterBuilder = clusterBuilder.withPort(port);
        clusterBuilder = clusterBuilder.withPoolingOptions(po);
        clusterBuilder = clusterBuilder.withLoadBalancingPolicy(new TokenAwarePolicy( DCAwareRoundRobinPolicy.builder().build()));

        if (null != username)
            clusterBuilder = clusterBuilder.withCredentials(username, password);
        if (null != truststore)
            clusterBuilder = clusterBuilder.withSSL(createSSLOptions());
        
        return true;
    }

    private SSLOptions createSSLOptions() 
        throws KeyStoreException, FileNotFoundException, IOException, NoSuchAlgorithmException, 
               KeyManagementException, CertificateException, UnrecoverableKeyException {
        TrustManagerFactory tmf = null;
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load((InputStream) new FileInputStream(new File(truststorePath)), 
                truststorePwd.toCharArray());
        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(tks);
    
        KeyManagerFactory kmf = null;
        if (null != keystorePath) {
            KeyStore kks = KeyStore.getInstance("JKS");
            kks.load((InputStream) new FileInputStream(new File(keystorePath)), 
                    keystorePwd.toCharArray());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(kks, keystorePwd.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf != null? kmf.getKeyManagers() : null, 
                        tmf != null ? tmf.getTrustManagers() : null, 
                        new SecureRandom());

        return JdkSSLOptions.builder().withSSLContext(sslContext).build();
    }
}
