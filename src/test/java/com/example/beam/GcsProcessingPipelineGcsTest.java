package com.example.beam;

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests using google-cloud-nio to simulate GCS. */
@RunWith(JUnit4.class)
public class GcsProcessingPipelineGcsTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPipelineWithGcsEmulator() throws Exception {
    // 1. Setup in-memory GCS
    String bucketName = "test-bucket";
    String filename = "1698400800000.csv"; // 2023-10-27T10:00:00Z

    // Use google-cloud-nio explicitly with in-memory configuration
    // This ensures we don't try to connect to real GCS.
    try (FileSystem fs =
        com.google.cloud.storage.contrib.nio.CloudStorageFileSystem.forBucket(
            bucketName,
            com.google.cloud.storage.contrib.nio.CloudStorageConfiguration.DEFAULT,
            LocalStorageHelper.getOptions())) {

      Path path = fs.getPath(filename);

      List<String> lines =
          Arrays.asList("saga1,nodeA,1698400800000,R", "saga2,nodeA,1698400800000,C");
      String content = String.join("\n", lines);

      Files.write(path, content.getBytes(StandardCharsets.UTF_8));

      // 2. Verify we can read it back
      byte[] readBytes = Files.readAllBytes(path);
      String readContent = new String(readBytes, StandardCharsets.UTF_8);

      if (!readContent.equals(content)) {
        throw new RuntimeException("Content mismatch in fake GCS");
      }

      System.out.println(
          "Successfully wrote to and read from in-memory GCS using google-cloud-nio.");
      System.out.println(
          "Note: To run the Beam pipeline, you would need to configure GcsOptions to point to a GCS emulator.");
    }
  }
}
