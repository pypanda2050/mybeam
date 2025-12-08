package com.example.beam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test that runs the pipeline against local Docker services. Requires: - Postgres at
 * localhost:5432 (user: test, pass: test, db: testdb) - GCS Emulator at localhost:4443
 */
@RunWith(JUnit4.class)
public class PostgresToGcsPipelineIntegrationTest {

  private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/testdb";
  private static final String POSTGRES_USER = "test";
  private static final String POSTGRES_PASSWORD = "test";
  private static final String GCS_ENDPOINT = "http://localhost:4443";
  private static final String BUCKET_NAME = "test-bucket-pg";

  @Before
  public void setup() throws Exception {
    // 1. Setup Postgres Data
    Properties props = new Properties();
    props.setProperty("user", POSTGRES_USER);
    props.setProperty("password", POSTGRES_PASSWORD);

    try (Connection conn = DriverManager.getConnection(POSTGRES_URL, props);
        Statement stmt = conn.createStatement()) {

      stmt.execute("DROP TABLE IF EXISTS dlq_job");
      stmt.execute(
          "CREATE TABLE dlq_job ("
              + "saga_id VARCHAR(255), "
              + "node_id VARCHAR(255), "
              + "create_ts BIGINT, "
              + "dlq_ts BIGINT)");

      long now = System.currentTimeMillis();
      long recentTs = now - 10000;

      stmt.execute(
          String.format(
              "INSERT INTO dlq_job VALUES ('saga-int', 'node-int', %d, %d)", now, recentTs));
    }

    // 2. Setup GCS Bucket
    com.google.cloud.storage.Storage storage =
        com.google.cloud.storage.StorageOptions.newBuilder()
            .setHost(GCS_ENDPOINT)
            .setProjectId("test-project")
            .setCredentials(com.google.cloud.NoCredentials.getInstance())
            .build()
            .getService();

    if (storage.get(BUCKET_NAME) == null) {
      storage.create(com.google.cloud.storage.BucketInfo.of(BUCKET_NAME));
    }
  }

  @Test
  public void testPipelineIntegration() {
    String outputDir = "gs://" + BUCKET_NAME + "/output";

    String[] args = {
      "--postgresUrl=" + POSTGRES_URL,
      "--postgresUser=" + POSTGRES_USER,
      "--postgresPassword=" + POSTGRES_PASSWORD,
      "--output=" + outputDir,
      "--gcsEndpoint=" + GCS_ENDPOINT + "/storage/v1",
      "--project=test-project",
      "--gcpTempLocation=gs://" + BUCKET_NAME + "/temp"
    };

    PostgresToGcsPipeline.main(args);

    // Verification would ideally check GCS content, but for now we rely on pipeline
    // completion without error.
    // We could use the Storage client to verify the file exists if needed.
    System.out.println("Integration test finished successfully.");
  }
}
