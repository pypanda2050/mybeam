package com.example.beam;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PostgresToGcsPipelineTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String JDBC_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
  private static final String USER = "sa";
  private static final String PASSWORD = "";
  private static final String DRIVER_CLASS = "org.h2.Driver";

  @Before
  public void setup() throws Exception {
    // Setup H2 Database
    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement stmt = conn.createStatement()) {

      stmt.execute("DROP TABLE IF EXISTS dlq_job");
      stmt.execute(
          "CREATE TABLE dlq_job ("
              + "saga_id VARCHAR(255), "
              + "node_id VARCHAR(255), "
              + "create_ts BIGINT, "
              + "dlq_ts BIGINT)");
    }
  }

  @Test
  public void testPipeline() throws Exception {
    // 1. Insert Test Data
    long now = System.currentTimeMillis();
    long recentTs = now - 10000; // 10 seconds ago
    long oldTs = now - 7200000; // 2 hours ago

    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
        Statement stmt = conn.createStatement()) {

      // Recent record (should be processed)
      stmt.execute(
          String.format("INSERT INTO dlq_job VALUES ('saga1', 'nodeA', %d, %d)", now, recentTs));

      // Old record (should be filtered out)
      stmt.execute(
          String.format("INSERT INTO dlq_job VALUES ('saga2', 'nodeB', %d, %d)", now, oldTs));
    }

    // 2. Define Output
    File outputDir = tmpFolder.newFolder("output");

    // 3. Run Pipeline
    String[] args = {
      "--postgresUrl=" + JDBC_URL,
      "--postgresUser=" + USER,
      "--postgresPassword=" + PASSWORD,
      "--driverClass=" + DRIVER_CLASS,
      "--output=" + outputDir.getAbsolutePath()
    };

    PostgresToGcsPipeline.main(args);

    // 4. Verify Output
    // Expected output file: dlq/{recentHour}.csv
    // We need to find the file in the output directory
    File dlqDir = new File(outputDir, "dlq");
    if (!dlqDir.exists()) {
      // It might be that FileIO writes directly to outputDir if naming is relative?
      // FileIO.writeDynamic().to(output) with naming("dlq/...")
      // If output is /tmp/output, file is /tmp/output/dlq/key.csv
      // Let's check the structure.
    }

    // Walk the directory to find the file
    // The key is dlq_hour (millis).
    // We expect one file.

    // Note: FileIO with local filesystem might behave slightly differently than
    // GCS.
    // But typically it creates the directory structure.

    // Let's just search for any .csv file in the output tree
    File[] files = outputDir.listFiles(); // This might be empty if it wrote to subdirs

    // Actually, let's just verify the content of the processed record.
    // Since we don't know the exact hour key easily without calculation (and
    // potential timezone issues in test env),
    // we can just look for the file containing "saga1".

    // Recursive search for csv files
    java.util.stream.Stream<java.nio.file.Path> walk = Files.walk(outputDir.toPath());
    List<String> allLines =
        walk.filter(path -> path.toString().endsWith(".csv"))
            .flatMap(
                path -> {
                  try {
                    return Files.readAllLines(path).stream();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(java.util.stream.Collectors.toList());

    if (allLines.isEmpty()) {
      throw new RuntimeException("No output files found.");
    }

    boolean foundSaga1 = allLines.stream().anyMatch(l -> l.contains("saga1"));
    boolean foundSaga2 = allLines.stream().anyMatch(l -> l.contains("saga2"));

    if (!foundSaga1) {
      throw new RuntimeException("Expected saga1 not found.");
    }
    if (foundSaga2) {
      throw new RuntimeException("Unexpected saga2 found (should be filtered).");
    }

    System.out.println("Test passed: Correctly processed recent records and filtered old ones.");
  }
}
