package com.example.beam;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class CassandraCustomReader {

  public interface CassandraOptions extends PipelineOptions {
    @Description("Comma-separated Cassandra contact hosts")
    @Required
    String getCassandraHosts();

    void setCassandraHosts(String value);

    @Description("Cassandra native transport port")
    @Default.Integer(9042)
    Integer getCassandraPort();

    void setCassandraPort(Integer value);

    @Description("Cassandra keyspace")
    @Required
    String getCassandraKeyspace();

    void setCassandraKeyspace(String value);

    @Description("Cassandra table — used for connection and as the token-range split target")
    @Required
    String getCassandraTable();

    void setCassandraTable(String value);

    @Description(
        "Inclusive lower bound for dlq_ts (epoch millis). "
            + "Records with dlq_ts < this value are dropped in a Beam Filter step after the read.")
    Long getDlqTsFrom();

    void setDlqTsFrom(Long value);

    @Description(
        "Inclusive upper bound for dlq_ts (epoch millis). "
            + "Records with dlq_ts > this value are dropped in a Beam Filter step after the read.")
    Long getDlqTsTo();

    void setDlqTsTo(Long value);

    @Description("Cassandra username (leave blank for no auth)")
    String getCassandraUsername();

    void setCassandraUsername(String value);

    @Description("Cassandra password")
    String getCassandraPassword();

    void setCassandraPassword(String value);

    @Description("Output GCS path prefix")
    @Required
    String getOutput();

    void setOutput(String value);
  }

  /**
   * Reads from Cassandra with full token-range parallel splitting and applies an optional
   * Beam-level post-read filter.
   *
   * <h3>Why no {@code withQuery()} for time-range filtering</h3>
   *
   * <p>CassandraIO's {@code ReadFn.buildInitialQuery} appends token-range predicates
   * ({@code AND (token(pk) >= ?) AND (token(pk) < ?)}) directly to whatever string is passed via
   * {@code withQuery()}. If that string ends with {@code ALLOW FILTERING}, the result is:
   *
   * <pre>
   *   ... WHERE dlq_ts >= X AND dlq_ts <= Y ALLOW FILTERING AND (token(pk) >= ?) ...
   * </pre>
   *
   * <p>which is invalid CQL. Omitting {@code withQuery()} lets CassandraIO use its default
   * {@code SELECT * FROM keyspace.table WHERE (token(pk) >= ?) AND (token(pk) < ?)} path,
   * preserving correct token-range splitting. The time-range predicate is then applied as a
   * {@link Filter#by} step in Beam, which runs on every worker after each Cassandra split is read.
   *
   * <p>If pushing the filter into Cassandra is required for performance, add a secondary index on
   * {@code dlq_ts} and pass a pre-built CQL string (without {@code ALLOW FILTERING}) via a
   * separate {@code withQuery()} call — but be aware that CassandraIO still appends token
   * predicates, so the query must not contain {@code ALLOW FILTERING}.
   */
  public static class CassandraRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final CassandraIO.Read<T> delegate;
    private final SerializableFunction<T, Boolean> postFilter;

    private CassandraRead(
        CassandraIO.Read<T> delegate, SerializableFunction<T, Boolean> postFilter) {
      this.delegate = delegate;
      this.postFilter = postFilter;
    }

    /**
     * Builds a {@code CassandraRead} from pipeline options.
     *
     * @param postFilter optional Beam-level predicate applied after reading; pass {@code null} to
     *     read all rows
     */
    public static <T> CassandraRead<T> fromOptions(
        CassandraOptions options,
        Class<T> entityClass,
        Coder<T> coder,
        SerializableFunction<T, Boolean> postFilter) {

      List<String> hosts = Arrays.asList(options.getCassandraHosts().trim().split("\\s*,\\s*"));

      // withTable() (no withQuery()) is intentional: it lets CassandraIO use its default
      // token-range split strategy, distributing work evenly across the Cassandra ring.
      CassandraIO.Read<T> read =
          CassandraIO.<T>read()
              .withHosts(hosts)
              .withPort(options.getCassandraPort())
              .withKeyspace(options.getCassandraKeyspace())
              .withTable(options.getCassandraTable())
              .withEntity(entityClass)
              .withCoder(coder);

      String username = options.getCassandraUsername();
      if (username != null && !username.isEmpty()) {
        read = read.withUsername(username).withPassword(options.getCassandraPassword());
      }

      return new CassandraRead<>(read, postFilter);
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      PCollection<T> records = input.apply("ReadFromCassandra", delegate);
      if (postFilter != null) {
        return records.apply("FilterByTimeRange", Filter.by(postFilter));
      }
      return records;
    }
  }

  /** Entity mapped to the {@code dlq_job} Cassandra table. */
  @Table(keyspace = "my_keyspace", name = "dlq_job")
  public static class DlqJob implements Serializable {

    @PartitionKey
    @Column(name = "saga_id")
    public String sagaId;

    @Column(name = "node_id")
    public String nodeId;

    @Column(name = "create_ts")
    public long createTs;

    @Column(name = "dlq_ts")
    public long dlqTs;
  }

  public static void main(String[] args) {
    CassandraOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CassandraOptions.class);

    Pipeline p = Pipeline.create(options);

    // Capture as finals so they can be referenced in the serializable lambda below.
    final Long from = options.getDlqTsFrom();
    final Long to = options.getDlqTsTo();

    // Build a Beam-level predicate only when at least one bound is set.
    // Both Long values are serializable, so the lambda serialises cleanly across workers.
    SerializableFunction<DlqJob, Boolean> timeFilter =
        (from != null || to != null)
            ? job -> (from == null || job.dlqTs >= from) && (to == null || job.dlqTs <= to)
            : null;

    p.apply(
            CassandraRead.fromOptions(
                options, DlqJob.class, SerializableCoder.of(DlqJob.class), timeFilter))
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
