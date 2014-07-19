/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.util.Map;

public class TransferManagerBuilder {

    public static final String S3_PROTOCOL = "S3_PROTOCOL";
    public static final String S3_PROXY = "S3_PROXY";
    public static final String S3_PROXY_PORT = "S3_PROXY_PORT";


    public static TransferManager buildTransferManager(Map conf) {
        Protocol protocol = Protocol.HTTPS;
        String proxy = null;
        int proxyPort = 0;
        if (conf.containsKey(S3_PROTOCOL)) {
            protocol = Protocol.valueOf((String) conf.get(S3_PROTOCOL));
        }
        if (conf.containsKey(S3_PROXY)) {
            proxy = (String) conf.get(S3_PROXY);
        }
        if (conf.containsKey(S3_PROXY_PORT)) {
            proxyPort = ((Long) conf.get(S3_PROXY_PORT)).intValue();
        }
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        AWSCredentials credentials = provider.getCredentials();
        ClientConfiguration config = new ClientConfiguration().withProtocol(protocol);
        if (proxy != null) {
            config.withProxyHost(proxy);
        }
        if (proxyPort != 0) {
            config.withProxyPort(proxyPort);
        }
        AmazonS3 client = new AmazonS3Client(credentials, config);
        return new TransferManager(client);
    }
}
