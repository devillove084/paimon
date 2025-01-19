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

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for OpenDALFileIO correctness. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OpenDALFileIOTest {

    //    private static final String BUCKET_NAME = "my-test-bucket";
    //    private OpenDALFileIO fileIO;
    //
    //    @BeforeAll
    //    static void createBucketIfNeeded() throws Exception {
    //
    //    }
    //
    //    @BeforeEach
    //    void setUp() {
    //        fileIO = new OpenDALFileIO();
    //        Map<String, String> options = new HashMap<>();
    //        options.put("opendal.s3.endpoint", "http://127.0.0.1:9000");
    //        options.put("opendal.s3.bucket", BUCKET_NAME);
    //        options.put("opendal.s3.access-key", "minio");
    //        options.put("opendal.s3.secret-key", "minio123");
    //        Options paimon_options = new Options(options);
    //        CatalogContext context = CatalogContext.create(paimon_options);
    //        fileIO.configure(context);
    //    }
    //
    //    @Test
    //    public void testWriteAndRead() throws IOException {
    //        Path testPath = new Path("/test-dir/test-file.txt");
    //        String content = "Hello from MinIO!";
    //
    //        // Write
    //        try (var out = fileIO.newOutputStream(testPath, true)) {
    //            out.write(content.getBytes());
    //        }
    //
    //        // Read
    //        var in = fileIO.newInputStream(testPath);
    //        byte[] buffer = new byte[content.length()];
    //        int read = in.read(buffer);
    //        in.close();
    //
    //        assertEquals(content.length(), read);
    //        assertEquals(content, new String(buffer));
    //    }
    //
    //    @Test
    //    public void testExistsAndDelete() throws IOException {
    //        Path testPath = new Path("/test-dir/delete-me.txt");
    //        String content = "Delete me";
    //
    //        // Write
    //        try (var out = fileIO.newOutputStream(testPath, true)) {
    //            out.write(content.getBytes());
    //        }
    //        // Check exists
    //        assertTrue(fileIO.exists(testPath));
    //
    //        // Delete
    //        fileIO.delete(testPath, false);
    //        // Now should not exist
    //        boolean exists = fileIO.exists(testPath);
    //        Assertions.assertFalse(exists, "File should be deleted and not exist anymore.");
    //    }
}
