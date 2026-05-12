package com.example.beam;

import com.example.beam.CassandraCustomReader.CassandraOptions;
import com.example.beam.CustomCassandraReaderWithFilteringCQL.CassandraReadWithCql;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link CassandraReadWithCql#buildFilterCql}.
 *
 * <p>These tests do not require a running Cassandra instance. They verify that the generated
 * CQL string has the correct structure for CassandraIO to safely append token-range predicates:
 * the string must end with a WHERE clause value — never with ALLOW FILTERING — so that
 * CassandraIO can append {@code AND (token(pk) >= ?) AND (token(pk) < ?)} without producing
 * invalid CQL.
 */
@RunWith(JUnit4.class)
public class CustomCassandraReaderWithFilteringCQLTest {

  private CassandraOptions options;

  @Before
  public void buildOptions() {
    options = PipelineOptionsFactory.as(CassandraOptions.class);
    options.setCassandraHosts("localhost");
    options.setCassandraPort(9042);
    options.setCassandraKeyspace("my_keyspace");
    options.setCassandraTable("dlq_job");
    options.setOutput("/tmp/out");
  }

  @Test
  public void testNoBoundsReturnsNull() {
    // No bounds → no withQuery() call → CassandraIO uses default SELECT *
    Assert.assertNull(CassandraReadWithCql.buildFilterCql(options));
  }

  @Test
  public void testBothBoundsProducesRangeClause() {
    options.setDlqTsFrom(1000L);
    options.setDlqTsTo(2000L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    Assert.assertNotNull(cql);
    Assert.assertTrue("CQL must contain dlq_ts >= lower", cql.contains("dlq_ts >= 1000"));
    Assert.assertTrue("CQL must contain dlq_ts <= upper", cql.contains("dlq_ts <= 2000"));
  }

  @Test
  public void testLowerBoundOnlyProducesGteClause() {
    options.setDlqTsFrom(500L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    Assert.assertNotNull(cql);
    Assert.assertTrue(cql.contains("dlq_ts >= 500"));
    Assert.assertFalse("Upper bound must not appear", cql.contains("dlq_ts <="));
  }

  @Test
  public void testUpperBoundOnlyProducesLteClause() {
    options.setDlqTsTo(3000L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    Assert.assertNotNull(cql);
    Assert.assertTrue(cql.contains("dlq_ts <= 3000"));
    Assert.assertFalse("Lower bound must not appear", cql.contains("dlq_ts >="));
  }

  @Test
  public void testCqlContainsCorrectKeyspaceAndTable() {
    options.setDlqTsFrom(1000L);
    options.setDlqTsTo(2000L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    Assert.assertTrue("CQL must reference keyspace and table",
        cql.contains("my_keyspace.dlq_job"));
  }

  @Test
  public void testCqlContainsWhereClause() {
    options.setDlqTsFrom(1000L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    // CassandraIO's buildInitialQuery looks for the literal string "WHERE" (case-insensitive)
    // to decide whether to append " AND" or " WHERE" before token predicates.
    Assert.assertTrue("CQL must contain WHERE so CassandraIO appends AND for token ranges",
        cql.toUpperCase().contains("WHERE"));
  }

  @Test
  public void testCqlDoesNotContainAllowFiltering() {
    options.setDlqTsFrom(1000L);
    options.setDlqTsTo(2000L);

    String cql = CassandraReadWithCql.buildFilterCql(options);

    // ALLOW FILTERING must NOT appear: CassandraIO appends token predicates after this string,
    // so ALLOW FILTERING in the middle would produce invalid CQL.
    Assert.assertFalse(
        "CQL must NOT contain ALLOW FILTERING — token-range predicates are appended after",
        cql.toUpperCase().contains("ALLOW FILTERING"));
  }

  @Test
  public void testCqlEndsWithPredicateNotKeyword() {
    options.setDlqTsFrom(1000L);
    options.setDlqTsTo(2000L);

    String cql = CassandraReadWithCql.buildFilterCql(options).trim();

    // The string must end with a numeric value so CassandraIO can safely append
    // " AND (token(pk) >= ?) AND (token(pk) < ?)"
    char last = cql.charAt(cql.length() - 1);
    Assert.assertTrue(
        "CQL must end with a digit (numeric bound) so token predicates can be appended",
        Character.isDigit(last));
  }
}
