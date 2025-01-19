/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.opendal;

import org.apache.paimon.catalog.CatalogContext;

import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;

import java.util.Map;

public class OperatorFactory {

    public static OperatorWrapper create(CatalogContext context) {
        Map<String, String> options = context.options().toMap();
        String endpoint = options.getOrDefault("opendal.s3.endpoint", "http://127.0.0.1:9000");
        String bucket = options.getOrDefault("opendal.s3.bucket", "my-test-bucket");
        String accessKey = options.getOrDefault("opendal.s3.access-key", "minio");
        String secretKey = options.getOrDefault("opendal.s3.secret-key", "minio123");

        final ServiceConfig.S3 builder =
                ServiceConfig.S3
                        .builder()
                        .endpoint(endpoint)
                        .bucket(bucket)
                        .region("auto")
                        .accessKeyId(accessKey)
                        .secretAccessKey(secretKey)
                        .build();
        return new OperatorWrapper(Operator.of(builder));
    }
}
