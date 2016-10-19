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
package org.apache.storm.s3.bolt;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.s3.output.S3Output;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class S3Bolt extends BaseRichBolt {

    private transient S3Output s3;
    private transient OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            this.collector = collector;
            s3 = new S3Output(stormConf);
            String componentId = context.getThisComponentId();
            int taskId = context.getThisTaskId();
            s3.withIdentifier(componentId + "-" + taskId);
            s3.prepare(stormConf);
        } catch (IOException e) {
            log.error("Error: " + e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            s3.write(tuple);
            this.collector.ack(tuple);
        } catch (IOException e) {
            log.warn("write/sync failed.", e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        log.info("declareOutputFields not implemented");
    }
}
