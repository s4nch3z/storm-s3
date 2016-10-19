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
package org.apache.storm.s3.trident.state;

import org.apache.storm.s3.output.trident.DefaultS3TransactionalOutput;
import org.apache.storm.s3.output.trident.S3TransactionalOutput;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class S3StateFactory implements StateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S3StateFactory.class);

    public S3StateFactory() {

    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        LOG.info("makeState(partitionIndex={}, numpartitions={}", partitionIndex, numPartitions);
        S3TransactionalOutput s3 = new DefaultS3TransactionalOutput(conf);
        try {
            s3.withBucketName("global-state");
            s3.prepare(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        S3State state = new S3State(s3);
        return state;
    }
}
