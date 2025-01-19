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

package org.apache.paimon.benchmark.fileio.s3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.s3.S3FileIO;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Single-thread "append write" benchmark for S3FileIO (Hadoop S3A). */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@Threads(1)
public class S3AppendWriteBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        /**
         * We declare a parameter for sizeInMB, so JMH will run the benchmark with each value in the
         * array separately.
         *
         * <p>- 100 means 100MB - 1024 means 1GB - 10240 means 10GB
         */
        @Param({"100", "1024"})
        public int sizeInMB;

        public S3FileIO fileIO;
        public Path path;
        public byte[] data;

        @Setup(Level.Trial)
        public void setup() {
            fileIO = new S3FileIO();

            Map<String, String> cfg = new HashMap<>();
            // Hadoop S3A style config
            // For example:
            cfg.put("fs.s3a.endpoint", "http://127.0.0.1:9000");
            cfg.put("fs.s3a.access.key", "minio");
            cfg.put("fs.s3a.secret.key", "minio123");
            cfg.put("fs.s3a.path.style.access", "true");

            Options options = new Options(cfg);
            CatalogContext context = CatalogContext.create(options);
            fileIO.configure(context);

            // path
            path = new Path("s3a://my-test-bucket/bench-write/test-file.bin");

            long fileSize = (long) sizeInMB * 1024 * 1024;
            data = new byte[(int) fileSize];
            Arrays.fill(data, (byte) 'A');
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            fileIO.delete(path, false);
        }
    }

    @Benchmark
    public void appendWrite(BenchmarkState state) throws IOException {
        try (PositionOutputStream out = state.fileIO.newOutputStream(state.path, true)) {
            out.write(state.data);
        }
    }
}
