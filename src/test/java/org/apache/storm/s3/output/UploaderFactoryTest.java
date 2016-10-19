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
package org.apache.storm.s3.output;

import org.apache.storm.Config;
import org.junit.Test;

import static org.junit.Assert.*;

public class UploaderFactoryTest {

    @Test
    public void testBuildUploader() throws Exception {
        Uploader uploader = UploaderFactory.buildUploader(new Config());
        assertTrue(uploader instanceof PutRequestUploader);
    }

    @Test
    public void testTransferManagerUploader() throws Exception {
        Config config = new Config();
        config.put(UploaderFactory.UPLOADER_CLASS, "org.apache.storm.s3.output.BlockingTransferManagerUploader");
        Uploader uploader = UploaderFactory.buildUploader(config);
        assertTrue(uploader instanceof BlockingTransferManagerUploader);
    }
}
