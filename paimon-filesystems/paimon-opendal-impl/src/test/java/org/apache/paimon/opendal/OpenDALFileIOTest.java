package org.apache.paimon.opendal;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for OpenDALFileIO correctness.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OpenDALFileIOTest {

    private static final String BUCKET_NAME = "my-test-bucket";
    private OpenDALFileIO fileIO;

    @BeforeAll
    static void createBucketIfNeeded() throws Exception {
//         S3Client s3Client = S3Client.builder()
//             .endpointOverride(URI.create("http://127.0.0.1:9000"))
//             .credentialsProvider(StaticCredentialsProvider.create(
//                 AwsBasicCredentials.create("minio","minio123")
//             ))
//             .region(Region.of("us-east-1"))
//             .build();
//
//         try {
//             s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
//         } catch (BucketAlreadyOwnedByYouException e) {
//         }
    }

    @BeforeEach
    void setUp() {
        // 构建OpenDALFileIO
        fileIO = new OpenDALFileIO();
        // 模拟 CatalogContext 中的配置
        Map<String, String> options = new HashMap<>();
        options.put("opendal.s3.endpoint", "http://127.0.0.1:9000");
        options.put("opendal.s3.bucket", BUCKET_NAME);
        options.put("opendal.s3.access-key", "minio");
        options.put("opendal.s3.secret-key", "minio123");
        CatalogContext context = CatalogContext.create(options);
        fileIO.configure(context);
    }

    @Test
    public void testWriteAndRead() throws IOException {
        Path testPath = new Path("/test-dir/test-file.txt");
        String content = "Hello from MinIO!";

        // Write
        try (var out = fileIO.newOutputStream(testPath, true)) {
            out.write(content.getBytes());
        }

        // Read
        var in = fileIO.newInputStream(testPath);
        byte[] buffer = new byte[content.length()];
        int read = in.read(buffer);
        in.close();

        assertEquals(content.length(), read);
        assertEquals(content, new String(buffer));
    }

    @Test
    public void testExistsAndDelete() throws IOException {
        Path testPath = new Path("/test-dir/delete-me.txt");
        String content = "Delete me";

        // Write
        try (var out = fileIO.newOutputStream(testPath, true)) {
            out.write(content.getBytes());
        }
        // Check exists
        assertTrue(fileIO.exists(testPath));

        // Delete
        fileIO.delete(testPath, false);
        // Now should not exist
        boolean exists = fileIO.exists(testPath);
        Assertions.assertFalse(exists, "File should be deleted and not exist anymore.");
    }
}
