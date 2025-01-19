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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Single-thread benchmark for "random read" using S3FileIO (Hadoop S3A).
 *
 * <p>We pre-write a file in setup, then do random reads during the benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@Threads(1)
public class S3RandomReadBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private static final int FILE_SIZE = 1073741824; // e.g. 1GB
        private static final int NUM_OPS = 1000; // do 1000 random reads

        public S3FileIO fileIO;
        public Path path;

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
            cfg.put("fs.s3a.endpoint", "http://127.0.0.1:9000");
            cfg.put("fs.s3a.access.key", "minio");
            cfg.put("fs.s3a.secret.key", "minio123");
            cfg.put("fs.s3a.path.style.access", "true");

            Options options = new Options(cfg);
            CatalogContext context = CatalogContext.create(options);
            fileIO.configure(context);

            // 2) Path for random read
            path = new Path("s3a://my-test-bucket/bench-read/rand-read.bin");

            // 3) Pre-write file
            byte[] data = new byte[FILE_SIZE];
            Arrays.fill(data, (byte) 'A');
            try (PositionOutputStream out = fileIO.newOutputStream(path, true)) {
                out.write(data);
            }
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            // optional: remove the file
            fileIO.delete(path, false);
        }
    }

    @Benchmark
    public void randomReadTest(BenchmarkState state) throws IOException {
        byte[] buffer = new byte[state.blockSize];
        try (SeekableInputStream in = state.fileIO.newInputStream(state.path)) {
            for (int i = 0; i < BenchmarkState.NUM_OPS; i++) {
                long offset =
                        ThreadLocalRandom.current()
                                .nextLong(BenchmarkState.FILE_SIZE - state.blockSize);
                in.seek(offset);
                int read = in.read(buffer, 0, state.blockSize);
                // The read might return fewer bytes if near end-of-file,
            }
        }
    }
}
