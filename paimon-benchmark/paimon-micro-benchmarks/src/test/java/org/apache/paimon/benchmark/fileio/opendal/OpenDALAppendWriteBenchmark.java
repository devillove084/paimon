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

package org.apache.paimon.benchmark.fileio.opendal;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.opendal.OpenDALFileIO;
import org.apache.paimon.options.Options;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Single-thread benchmark for "append write" using OpenDALFileIO.
 *
 * <p>It writes a file from scratch each time in the benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@Threads(1) // single-thread
public class OpenDALAppendWriteBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"100", "1024"})
        public int sizeInMB;

        public OpenDALFileIO fileIO;
        public Path writePath;
        public byte[] data;

        @Setup(Level.Trial)
        public void setup() {
            // 1) Initialize OpenDALFileIO
            fileIO = new OpenDALFileIO();
            Map<String, String> options = new HashMap<>();
            options.put("opendal.s3.endpoint", "http://127.0.0.1:9000");
            options.put("opendal.s3.bucket", "my-test-bucket");
            options.put("opendal.s3.access-key", "minio");
            options.put("opendal.s3.secret-key", "minio123");

            Options paimonOptions = new Options(options);
            CatalogContext context = CatalogContext.create(paimonOptions);
            fileIO.configure(context);

            // 2) Target path in MinIO. Leading "/" => /test-write/xxx
            writePath = new Path("/opendal-bench/write-test.bin");

            // 3) Prepare data
            data = new byte[sizeInMB * 1024 * 1024];
            Arrays.fill(data, (byte) 'A');
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            // Optionally delete the file
            fileIO.delete(writePath, false);
        }
    }

    @Benchmark
    public void appendWriteTest(BenchmarkState state) throws IOException {
        // Single operation => write entire data
        try (PositionOutputStream out = state.fileIO.newOutputStream(state.writePath, true)) {
            out.write(state.data);
        }
    }
}
