package com.example.beam;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test that runs the pipeline against a local GCS emulator.
 * Requires a GCS emulator (e.g., fake-gcs-server) running at http://localhost:4443.
 */
@RunWith(JUnit4.class)
public class GcsProcessingPipelineIntegrationTest {

    @Test
    public void testPipelineWithEmulator() throws Exception {
        // Emulator endpoint
        String gcsEndpoint = "http://localhost:4443";
        String bucketName = "test-bucket";
        String projectId = "test-project";
        
        // 1. Setup Data in Emulator
        // Configure Storage client to point to emulator
        com.google.cloud.storage.Storage storage = com.google.cloud.storage.StorageOptions.newBuilder()
                .setHost(gcsEndpoint)
                .setProjectId(projectId)
                .setCredentials(com.google.cloud.NoCredentials.getInstance())
                .build()
                .getService();
        
        // Create bucket if not exists
        if (storage.get(bucketName) == null) {
            storage.create(com.google.cloud.storage.BucketInfo.of(bucketName));
        }
        
        // Create input file
        String filename = "1698400800000.csv";
        com.google.cloud.storage.BlobId blobId = com.google.cloud.storage.BlobId.of(bucketName, filename);
        com.google.cloud.storage.BlobInfo blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(blobId).build();
        
        String content = "saga1,nodeA,1698400800000,R\n" +
                         "saga2,nodeA,1698400800000,C";
        storage.create(blobInfo, content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        
        // Input/Output paths
        String inputPattern = "gs://" + bucketName + "/*.csv";
        String outputDir = "gs://" + bucketName + "/output";
        
        // Arguments to configure GcsOptions
        String[] args = {
                "--project=" + projectId,
                "--gcpTempLocation=gs://" + bucketName + "/temp",
                "--input=" + inputPattern,
                "--output=" + outputDir,
                "--gcsEndpoint=" + gcsEndpoint + "/storage/v1"
        };
        
        // Run the pipeline
        GcsProcessingPipeline.main(args);
    }
}
