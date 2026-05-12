package com.example.beam;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.example.beam.CassandraCustomReader.CassandraOptions;
import com.example.beam.CassandraCustomReader.DlqJob;
import com.example.beam.CustomCassandraReaderWithFilteringCQL.CassandraReadWithCql;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link CustomCassandraReaderWithFilteringCQL} against a real Cassandra
 * instance.
 *
 * <p>Requires Cassandra 4.1 at localhost:9042 with a secondary index on {@code dlq_ts}:
 *
 * <pre>
 *   docker-compose up -d cassandra
 * </pre>
 *
 * The secondary index is created automatically in {@link #createSchemaAndIndex()}.
 * Without it, Cassandra rejects the filtered CQL with {@code InvalidQueryException}.
 */
@RunWith(JUnit4.class)
public class CustomCassandraReaderWithFilteringCQLIntegrationTest {

  private static final String CASSANDRA_HOST = "localhost";
  private static final int CASSANDRA_PORT = 9042;
  private static final String KEYSPACE = "my_keyspace";
  private static final String TABLE = "dlq_job";

  private static Cluster cluster;
  private static Session session;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  // ── schema setup ─────────────────────────────────────────────────────────

  @BeforeClass
  public static void createSchemaAndIndex() throws Exception {
    Exception lastException = null;
    for (int attempt = 0; attempt < 20; attempt++) {
      try {
        cluster =
            Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_PORT).build();
        session = cluster.connect();

        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS "
                + KEYSPACE
                + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute(
            "CREATE TABLE IF NOT EXISTS "
                + KEYSPACE
                + "."
                + TABLE
                + " (saga_id text PRIMARY KEY, node_id text, create_ts bigint, dlq_ts bigint)");

        // A SASI index is required because buildFilterCql() uses range operators (>= / <=)
        // and omits ALLOW FILTERING. Regular secondary indexes only support equality (=),
        // which means range predicates on dlq_ts would fail without ALLOW FILTERING or SASI.
        // SASI (SSTable Attached Secondary Index) with SPARSE mode supports numeric range scans.
        // SASI must be enabled in cassandra.yaml (enable_sasi_indexes: true) — the
        // docker-compose entrypoint sets this before Cassandra starts.
        session.execute(
            "CREATE CUSTOM INDEX IF NOT EXISTS dlq_ts_sasi ON "
                + KEYSPACE
                + "."
                + TABLE
                + " (dlq_ts) USING 'org.apache.cassandra.index.sasi.SASIIndex'"
                + " WITH OPTIONS = {'mode': 'SPARSE'}");
        return;
      } catch (Exception e) {
        lastException = e;
        Thread.sleep(3000);
      }
    }
    throw new RuntimeException(
        "Cassandra not available at " + CASSANDRA_HOST + ":" + CASSANDRA_PORT, lastException);
  }

  @AfterClass
  public static void closeCassandraConnection() {
    if (session != null) session.close();
    if (cluster != null) cluster.close();
  }

  @Before
  public void resetTestData() {
    session.execute("TRUNCATE " + KEYSPACE + "." + TABLE);
    //             sagaId    nodeId   createTs  dlqTs
    insert("saga1", "node1", 100L, 1500L); // inside  [1000, 2000]
    insert("saga2", "node2", 200L, 1000L); // at lower bound
    insert("saga3", "node3", 300L, 2000L); // at upper bound
    insert("saga4", "node4", 400L, 999L);  // below lower bound
    insert("saga5", "node5", 500L, 2001L); // above upper bound
  }

  private void insert(String sagaId, String nodeId, long createTs, long dlqTs) {
    session.execute(
        String.format(
            "INSERT INTO %s.%s (saga_id, node_id, create_ts, dlq_ts) VALUES ('%s','%s',%d,%d)",
            KEYSPACE, TABLE, sagaId, nodeId, createTs, dlqTs));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static class DlqJobToCsv extends SimpleFunction<DlqJob, String> {
    @Override
    public String apply(DlqJob job) {
      return job.sagaId + "," + job.nodeId + "," + job.createTs + "," + job.dlqTs;
    }
  }

  private List<String> runAndCollectSagaIds(Long from, Long to, File outputDir) throws Exception {
    CassandraOptions options = PipelineOptionsFactory.as(CassandraOptions.class);
    options.setCassandraHosts(CASSANDRA_HOST);
    options.setCassandraPort(CASSANDRA_PORT);
    options.setCassandraKeyspace(KEYSPACE);
    options.setCassandraTable(TABLE);
    options.setDlqTsFrom(from);
    options.setDlqTsTo(to);
    options.setOutput(outputDir.getAbsolutePath());

    Pipeline p = Pipeline.create(options);
    p.apply(
            CassandraReadWithCql.fromOptions(
                options, DlqJob.class, SerializableCoder.of(DlqJob.class)))
        .apply(MapElements.via(new DlqJobToCsv()))
        .apply(
            TextIO.write()
                .to(outputDir.getAbsolutePath() + "/out")
                .withSuffix(".csv")
                .withNumShards(1));
    p.run().waitUntilFinish();

    return Files.walk(outputDir.toPath())
        .filter(path -> path.toString().endsWith(".csv"))
        .flatMap(
            path -> {
              try {
                return Files.readAllLines(path).stream();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .filter(line -> !line.isBlank())
        .map(line -> line.split(",")[0])
        .collect(Collectors.toList());
  }

  // ── tests ─────────────────────────────────────────────────────────────────

  @Test
  public void testReadAllRowsWhenNoBoundsSet() throws Exception {
    List<String> sagaIds = runAndCollectSagaIds(null, null, tmpFolder.newFolder("no-filter"));

    Assert.assertEquals("Expected all 5 rows", 5, sagaIds.size());
  }

  @Test
  public void testBothBoundsFilterServerSide() throws Exception {
    // CQL pushed to Cassandra: SELECT * FROM ks.dlq_job WHERE dlq_ts >= 1000 AND dlq_ts <= 2000
    // CassandraIO adds per-split: AND (token(saga_id) >= ?) AND (token(saga_id) < ?)
    // Secondary index on dlq_ts makes this valid without ALLOW FILTERING.
    List<String> sagaIds =
        runAndCollectSagaIds(1000L, 2000L, tmpFolder.newFolder("both-bounds"));

    Assert.assertEquals("Expected 3 rows in [1000, 2000]", 3, sagaIds.size());
    Assert.assertTrue("saga1 (dlqTs=1500)", sagaIds.contains("saga1"));
    Assert.assertTrue("saga2 (dlqTs=1000, lower bound inclusive)", sagaIds.contains("saga2"));
    Assert.assertTrue("saga3 (dlqTs=2000, upper bound inclusive)", sagaIds.contains("saga3"));
    Assert.assertFalse("saga4 (dlqTs=999) must be excluded", sagaIds.contains("saga4"));
    Assert.assertFalse("saga5 (dlqTs=2001) must be excluded", sagaIds.contains("saga5"));
  }

  @Test
  public void testLowerBoundOnlyExcludesRowsBelow() throws Exception {
    List<String> sagaIds =
        runAndCollectSagaIds(1000L, null, tmpFolder.newFolder("lower-only"));

    Assert.assertEquals("Expected 4 rows with dlqTs >= 1000", 4, sagaIds.size());
    Assert.assertFalse("saga4 (dlqTs=999) must be excluded", sagaIds.contains("saga4"));
  }

  @Test
  public void testUpperBoundOnlyExcludesRowsAbove() throws Exception {
    List<String> sagaIds =
        runAndCollectSagaIds(null, 2000L, tmpFolder.newFolder("upper-only"));

    Assert.assertEquals("Expected 4 rows with dlqTs <= 2000", 4, sagaIds.size());
    Assert.assertFalse("saga5 (dlqTs=2001) must be excluded", sagaIds.contains("saga5"));
  }

  @Test
  public void testNoRowsReturnedWhenRangeMatchesNothing() throws Exception {
    List<String> sagaIds =
        runAndCollectSagaIds(9000L, 9999L, tmpFolder.newFolder("empty-range"));

    Assert.assertTrue("Expected empty result for out-of-range window", sagaIds.isEmpty());
  }

  @Test
  public void testPointRangeMatchesSingleRow() throws Exception {
    List<String> sagaIds =
        runAndCollectSagaIds(1000L, 1000L, tmpFolder.newFolder("point-range"));

    Assert.assertEquals(1, sagaIds.size());
    Assert.assertTrue("saga2 (dlqTs=1000) must match", sagaIds.contains("saga2"));
  }
}
