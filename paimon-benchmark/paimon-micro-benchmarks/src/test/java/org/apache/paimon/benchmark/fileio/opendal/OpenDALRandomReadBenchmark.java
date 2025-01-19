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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.opendal.OpenDALFileIO;
import org.apache.paimon.options.Options;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Single-thread benchmark for "random read" using OpenDALFileIO.
 *
 * <p>We pre-write a file in setup, then do random reads in the benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@Threads(1)
public class OpenDALRandomReadBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private static final int FILE_SIZE = 1073741824;
        private static final int NUM_OPS = 1000; // 1000 random reads

        public OpenDALFileIO fileIO;
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
            // 1) Initialize
            fileIO = new OpenDALFileIO();
            Map<String, String> options = new HashMap<>();
            options.put("opendal.s3.endpoint", "http://127.0.0.1:9000");
            options.put("opendal.s3.bucket", "my-test-bucket");
            options.put("opendal.s3.access-key", "minio");
            options.put("opendal.s3.secret-key", "minio123");

            Options paimonOptions = new Options(options);
            CatalogContext context = CatalogContext.create(paimonOptions);
            fileIO.configure(context);

            // 2) Path
            readPath = new Path("/opendal-bench/rand-read.bin");

            // 3) Pre-write a 1GB file for random read test
            byte[] data = new byte[FILE_SIZE];
            Arrays.fill(data, (byte) 'A');
            try (PositionOutputStream out = fileIO.newOutputStream(readPath, true)) {
                out.write(data);
            }
        }

        @TearDown(Level.Trial)
        public void teardown() throws IOException {
            fileIO.delete(readPath, false);
        }
    }

    @Benchmark
    public void randomReadTest(BenchmarkState state) throws IOException {
        // random read: do 1000 random reads of 4KB each
        byte[] buffer = new byte[state.blockSize];

        try (SeekableInputStream in = state.fileIO.newInputStream(state.readPath)) {
            for (int i = 0; i < BenchmarkState.NUM_OPS; i++) {
                long offset =
                        ThreadLocalRandom.current()
                                .nextLong(BenchmarkState.FILE_SIZE - state.blockSize);
                in.seek(offset);
                in.read(buffer, 0, state.blockSize);
            }
        }
    }
}
