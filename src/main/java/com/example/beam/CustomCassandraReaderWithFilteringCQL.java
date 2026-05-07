package com.example.beam;

import com.example.beam.CassandraCustomReader.CassandraOptions;
import com.example.beam.CassandraCustomReader.DlqJob;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * A Cassandra reader that pushes the {@code dlq_ts} time-range filter down into the CQL query
 * while still relying on CassandraIO's automatic token-range splitting for parallelism.
 *
 * <h3>How token-range splitting works with a custom query</h3>
 *
 * <p>CassandraIO's {@code ReadFn.buildInitialQuery} builds the per-split query like this:
 *
 * <ol>
 *   <li>If {@code withQuery()} is set and the string contains {@code WHERE}, it appends {@code AND}
 *       and then the token-range predicate {@code (token(pk) >= ?) AND (token(pk) < ?)}.
 *   <li>If {@code withQuery()} is set but the string has no {@code WHERE}, it appends {@code WHERE}
 *       followed by the token predicate.
 *   <li>If {@code withQuery()} is not set, it generates the default
 *       {@code SELECT * FROM keyspace.table WHERE (token(pk) >= ?) ...} automatically.
 * </ol>
 *
 * <p>Given our injected CQL:
 *
 * <pre>
 *   SELECT * FROM my_keyspace.dlq_job WHERE dlq_ts >= 1000 AND dlq_ts <= 2000
 * </pre>
 *
 * CassandraIO produces the following query per token-range split:
 *
 * <pre>
 *   SELECT * FROM my_keyspace.dlq_job
 *     WHERE dlq_ts >= 1000 AND dlq_ts <= 2000
 *     AND (token(saga_id) >= ?) AND (token(saga_id) < ?)
 * </pre>
 *
 * <h3>Why ALLOW FILTERING must NOT be in the injected CQL</h3>
 *
 * <p>{@code ALLOW FILTERING} appended to our CQL would land in the middle of the final query,
 * making it invalid:
 *
 * <pre>
 *   ... WHERE dlq_ts >= X ALLOW FILTERING AND (token(saga_id) >= ?) ...  ← invalid CQL
 * </pre>
 *
 * <h3>Schema requirement</h3>
 *
 * <p>Because {@code dlq_ts} is not the partition key, Cassandra requires a secondary index on
 * that column. Without it the query will fail with
 * {@code InvalidQueryException: Cannot execute this query as it might involve data filtering}.
 * Create the index once:
 *
 * <pre>
 *   CREATE INDEX ON my_keyspace.dlq_job (dlq_ts);
 * </pre>
 */
public class CustomCassandraReaderWithFilteringCQL {

  /**
   * PTransform that reads from Cassandra with a server-side {@code dlq_ts} filter and
   * full token-range parallel splitting.
   *
   * <p>Both {@code withTable()} and {@code withQuery()} are set simultaneously:
   * {@code withTable()} gives CassandraIO the table it needs to compute token ranges;
   * {@code withQuery()} supplies the time-range WHERE clause that is merged with those ranges.
   */
  public static class CassandraReadWithCql<T> extends PTransform<PBegin, PCollection<T>> {

    private final CassandraIO.Read<T> delegate;

    private CassandraReadWithCql(CassandraIO.Read<T> delegate) {
      this.delegate = delegate;
    }

    public static <T> CassandraReadWithCql<T> fromOptions(
        CassandraOptions options, Class<T> entityClass, Coder<T> coder) {

      List<String> hosts = Arrays.asList(options.getCassandraHosts().trim().split("\\s*,\\s*"));

      CassandraIO.Read<T> read =
          CassandraIO.<T>read()
              .withHosts(hosts)
              .withPort(options.getCassandraPort())
              .withKeyspace(options.getCassandraKeyspace())
              // withTable() is required even alongside withQuery() so that CassandraIO can
              // query Cassandra's system tables for token ranges to drive splitting.
              .withTable(options.getCassandraTable())
              .withEntity(entityClass)
              .withCoder(coder);

      String filterCql = buildFilterCql(options);
      if (filterCql != null) {
        read = read.withQuery(filterCql);
      }

      String username = options.getCassandraUsername();
      if (username != null && !username.isEmpty()) {
        read = read.withUsername(username).withPassword(options.getCassandraPassword());
      }

      return new CassandraReadWithCql<>(read);
    }

    /**
     * Builds the CQL SELECT with a {@code dlq_ts} WHERE clause from pipeline options.
     *
     * <p>{@code ALLOW FILTERING} is intentionally omitted: CassandraIO appends token-range
     * predicates directly after this string, so {@code ALLOW FILTERING} would appear mid-clause
     * and produce invalid CQL. A secondary index on {@code dlq_ts} is required instead.
     *
     * <p>Returns {@code null} when neither bound is set, allowing CassandraIO to use its default
     * full-table scan query.
     */
    static String buildFilterCql(CassandraOptions options) {
      Long from = options.getDlqTsFrom();
      Long to = options.getDlqTsTo();

      if (from == null && to == null) {
        return null;
      }

      String base =
          String.format(
              "SELECT * FROM %s.%s WHERE",
              options.getCassandraKeyspace(), options.getCassandraTable());

      if (from != null && to != null) {
        return String.format("%s dlq_ts >= %d AND dlq_ts <= %d", base, from, to);
      } else if (from != null) {
        return String.format("%s dlq_ts >= %d", base, from);
      } else {
        return String.format("%s dlq_ts <= %d", base, to);
      }
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.apply("ReadFromCassandraWithCql", delegate);
    }
  }

  public static void main(String[] args) {
    CassandraOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CassandraOptions.class);

    Pipeline p = Pipeline.create(options);

    p.apply(
            CassandraReadWithCql.fromOptions(
                options, DlqJob.class, SerializableCoder.of(DlqJob.class)))
        .apply(
            "FormatCsv",
            MapElements.via(
                new SimpleFunction<DlqJob, String>() {
                  @Override
                  public String apply(DlqJob job) {
                    return job.sagaId
                        + ","
                        + job.nodeId
                        + ","
                        + job.createTs
                        + ","
                        + job.dlqTs;
                  }
                }))
        .apply(
            "WriteToGcs",
            TextIO.write()
                .to(options.getOutput() + "/cassandra/dlq")
                .withSuffix(".csv")
                .withNumShards(1));

    p.run();
  }
}
