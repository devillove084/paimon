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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.s3.S3FileIO;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Single-thread benchmark for "sequential read" using S3FileIO (Hadoop S3A).
 *
 * <p>We pre-write a file in setup, then read it during the benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@Threads(1)
public class S3SequentialReadBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        // Adjust file size as needed, e.g. 1 GB
        private static final int FILE_SIZE = 1073741824;

        public S3FileIO fileIO;
        public Path readPath;

        /**
         * Here we define a set of block sizes (in bytes) from 1K to 16MB, doubling each time.
         * 1K=1024, 2K=2048, 4K=4096, ..., 16MB=16777216
         */
        @Param({
            "1024", //   1KB
            "2048", //   2KB
            "4096", //   4KB
            "8192", //   8KB
            "16384", //  16KB
            "32768", //  32KB
            "65536", //  64KB
            "131072", // 128KB
            "262144", // 256KB
            "524288", // 512KB
            "1048576", //   1MB
            "2097152", //   2MB
            "4194304", //   4MB
            "8388608", //   8MB
            "16777216" //  16MB
        })
        public int blockSize;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            // 1) Initialize S3FileIO
            fileIO = new S3FileIO();

            Map<String, String> cfg = new HashMap<>();
            // Example config for local MinIO:
            cfg.put("fs.s3a.endpoint", "http://127.0.0.1:9000");
            cfg.put("fs.s3a.access.key", "minio");
            cfg.put("fs.s3a.secret.key", "minio123");
            cfg.put("fs.s3a.path.style.access", "true");

            Options options = new Options(cfg);
            CatalogContext context = CatalogContext.create(options);
            fileIO.configure(context);

            // 2) Path to read from
            readPath = new Path("s3a://my-test-bucket/bench-read/seq-read.bin");

            // 3) Pre-write the file
            byte[] data = new byte[FILE_SIZE];
            Arrays.fill(data, (byte) 'A');
            try (PositionOutputStream out = fileIO.newOutputStream(readPath, true)) {
                out.write(data);
            }
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            // Delete after testing, optional
            fileIO.delete(readPath, false);
        }
    }

    @Benchmark
    public void sequentialReadTest(BenchmarkState state) throws IOException {
        byte[] buffer = new byte[state.blockSize]; // 8KB read chunk
        long totalRead = 0;
        try (SeekableInputStream in = state.fileIO.newInputStream(state.readPath)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                totalRead += read;
            }
        }
        assertEquals(BenchmarkState.FILE_SIZE, totalRead);
    }
}
