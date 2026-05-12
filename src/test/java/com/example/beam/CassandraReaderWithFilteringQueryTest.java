package com.example.beam;

import com.example.beam.CassandraCustomReader.CassandraOptions;
import com.example.beam.CassandraCustomReader.DlqJob;
import com.example.beam.CassandraReaderWithFilteringQuery.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the pure-function helpers in {@link CassandraReaderWithFilteringQuery}.
 *
 * <p>These tests do not require a running Cassandra instance. They verify:
 *
 * <ul>
 *   <li>{@link Read#buildFilterExpression} — produces the correct WHERE-clause predicates
 *       (without the {@code WHERE} keyword) for all combinations of {@code dlqTsFrom} /
 *       {@code dlqTsTo}, and returns {@code null} when neither is set.
 *   <li>{@link CassandraReaderWithFilteringQuery#buildCql} — produces structurally valid CQL
 *       with the column filter, token-range predicate, and {@code ALLOW FILTERING} in the
 *       correct order.
 *   <li>{@link CassandraReaderWithFilteringQuery#resolvePartitionKeyColumn} — reads
 *       {@code @PartitionKey} / {@code @Column} annotations to find the column name.
 * </ul>
 */
@RunWith(JUnit4.class)
public class CassandraReaderWithFilteringQueryTest {

  private static final String KEYSPACE = "my_keyspace";
  private static final String TABLE    = "dlq_job";
  private static final String PK_COL   = "saga_id";
  private static final long   TOKEN_LO = -9_000_000_000_000_000_000L;
  private static final long   TOKEN_HI = -4_500_000_000_000_000_000L;

  private CassandraOptions options;

  @Before
  public void buildOptions() {
    options = PipelineOptionsFactory.as(CassandraOptions.class);
    options.setCassandraHosts("localhost");
    options.setCassandraPort(9042);
    options.setCassandraKeyspace(KEYSPACE);
    options.setCassandraTable(TABLE);
    options.setOutput("/tmp/out");
  }

  // ── buildFilterExpression ────────────────────────────────────────────────

  @Test
  public void testBuildFilterExpressionNoBoundsReturnsNull() {
    Assert.assertNull(
        "No bounds should produce null (no column filter)",
        CassandraReaderWithFilteringQuery.buildFilterExpression(options));
  }

  @Test
  public void testBuildFilterExpressionBothBounds() {
    options.setDlqTsFrom(1_000L);
    options.setDlqTsTo(2_000L);

    String expr = CassandraReaderWithFilteringQuery.buildFilterExpression(options);

    Assert.assertNotNull(expr);
    Assert.assertTrue("Must include lower bound", expr.contains("dlq_ts >= 1000"));
    Assert.assertTrue("Must include upper bound", expr.contains("dlq_ts <= 2000"));
    Assert.assertFalse("Must not include WHERE keyword", expr.toUpperCase().contains("WHERE"));
    Assert.assertFalse("Must not include SELECT",        expr.toUpperCase().contains("SELECT"));
  }

  @Test
  public void testBuildFilterExpressionLowerBoundOnly() {
    options.setDlqTsFrom(500L);

    String expr = CassandraReaderWithFilteringQuery.buildFilterExpression(options);

    Assert.assertNotNull(expr);
    Assert.assertTrue(expr.contains("dlq_ts >= 500"));
    Assert.assertFalse("Upper bound must not appear", expr.contains("dlq_ts <="));
  }

  @Test
  public void testBuildFilterExpressionUpperBoundOnly() {
    options.setDlqTsTo(3_000L);

    String expr = CassandraReaderWithFilteringQuery.buildFilterExpression(options);

    Assert.assertNotNull(expr);
    Assert.assertTrue(expr.contains("dlq_ts <= 3000"));
    Assert.assertFalse("Lower bound must not appear", expr.contains("dlq_ts >="));
  }

  // ── buildCql ────────────────────────────────────────────────────────────

  @Test
  public void testBuildCqlNoFilterOmitsAllowFiltering() {
    // No column filter → token-range scan only; ALLOW FILTERING must NOT appear.
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, null, TOKEN_LO, TOKEN_HI);

    Assert.assertFalse(
        "ALLOW FILTERING must be absent when there is no column filter",
        cql.toUpperCase().contains("ALLOW FILTERING"));
    Assert.assertTrue("Must still contain token range",
        cql.contains("token(" + PK_COL + ")"));
  }

  @Test
  public void testBuildCqlNoFilterContainsTokenRange() {
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, null, TOKEN_LO, TOKEN_HI);

    Assert.assertTrue(cql.contains("token(" + PK_COL + ") >= " + TOKEN_LO));
    Assert.assertTrue(cql.contains("token(" + PK_COL + ") < "  + TOKEN_HI));
  }

  @Test
  public void testBuildCqlWithFilterContainsAllowFilteringLast() {
    String filterExpr = "dlq_ts >= 1000 AND dlq_ts <= 2000";
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, filterExpr, TOKEN_LO, TOKEN_HI).trim();

    Assert.assertTrue("ALLOW FILTERING must be present with a column filter",
        cql.toUpperCase().contains("ALLOW FILTERING"));
    // ALLOW FILTERING must be the very last clause so CassandraIO (if used) or Cassandra
    // itself does not encounter it before the token predicates.
    Assert.assertTrue(
        "ALLOW FILTERING must appear after the token-range predicates",
        cql.toUpperCase().endsWith("ALLOW FILTERING"));
  }

  @Test
  public void testBuildCqlColumnFilterAppearsBeforeTokenRange() {
    String filterExpr = "dlq_ts >= 1000 AND dlq_ts <= 2000";
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, filterExpr, TOKEN_LO, TOKEN_HI);

    int filterPos = cql.indexOf("dlq_ts");
    int tokenPos  = cql.indexOf("token(");
    Assert.assertTrue(
        "Column filter must appear before token-range predicates in the CQL",
        filterPos < tokenPos);
  }

  @Test
  public void testBuildCqlContainsCorrectKeyspaceAndTable() {
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, "dlq_ts >= 1", TOKEN_LO, TOKEN_HI);

    Assert.assertTrue(
        "CQL must reference the fully-qualified table name",
        cql.contains(KEYSPACE + "." + TABLE));
  }

  @Test
  public void testBuildCqlContainsSelectStar() {
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, null, TOKEN_LO, TOKEN_HI);

    Assert.assertTrue("CQL must start with SELECT *", cql.startsWith("SELECT *"));
  }

  @Test
  public void testBuildCqlContainsWhereKeyword() {
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, null, TOKEN_LO, TOKEN_HI);

    Assert.assertTrue("CQL must contain WHERE",
        cql.toUpperCase().contains("WHERE"));
  }

  @Test
  public void testBuildCqlBothBoundsReflectedInOutput() {
    String filterExpr = "dlq_ts >= 1000 AND dlq_ts <= 2000";
    String cql = CassandraReaderWithFilteringQuery.buildCql(
        KEYSPACE, TABLE, PK_COL, filterExpr, TOKEN_LO, TOKEN_HI);

    Assert.assertTrue(cql.contains("dlq_ts >= 1000"));
    Assert.assertTrue(cql.contains("dlq_ts <= 2000"));
    Assert.assertTrue(cql.contains("token(" + PK_COL + ") >= " + TOKEN_LO));
    Assert.assertTrue(cql.contains("token(" + PK_COL + ") < "  + TOKEN_HI));
  }

  // ── resolvePartitionKeyColumn ────────────────────────────────────────────

  @Test
  public void testResolvePartitionKeyColumnFromDlqJob() {
    // DlqJob.sagaId is annotated with @PartitionKey and @Column(name = "saga_id")
    String col = CassandraReaderWithFilteringQuery.resolvePartitionKeyColumn(DlqJob.class);
    Assert.assertEquals("saga_id", col);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResolvePartitionKeyColumnThrowsWhenMissing() {
    // A class with no @PartitionKey annotation should throw.
    CassandraReaderWithFilteringQuery.resolvePartitionKeyColumn(Object.class);
  }
}
