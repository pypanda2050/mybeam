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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
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

    @Description("Cassandra table — also used as the token-range split target when withQuery is set")
    @Required
    String getCassandraTable();

    void setCassandraTable(String value);

    @Description(
        "Full CQL SELECT to run instead of the default 'SELECT * FROM keyspace.table'. "
            + "Must include a complete SELECT … FROM … statement; token-range predicates are "
            + "appended automatically by CassandraIO for parallel reads.")
    String getCassandraCql();

    void setCassandraCql(String value);

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
   * Wraps {@link CassandraIO.Read} and injects the CQL from {@link CassandraOptions#getCassandraCql()},
   * so the query can be overridden at launch time via --cassandraCql without recompiling.
   *
   * <p>When {@code cassandraCql} is blank, CassandraIO falls back to its default
   * {@code SELECT * FROM keyspace.table} scan.
   */
  public static class CassandraRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final CassandraIO.Read<T> delegate;

    private CassandraRead(CassandraIO.Read<T> delegate) {
      this.delegate = delegate;
    }

    public static <T> CassandraRead<T> fromOptions(
        CassandraOptions options, Class<T> entityClass, Coder<T> coder) {

      List<String> hosts = Arrays.asList(options.getCassandraHosts().trim().split("\\s*,\\s*"));

      CassandraIO.Read<T> read =
          CassandraIO.<T>read()
              .withHosts(hosts)
              .withPort(options.getCassandraPort())
              .withKeyspace(options.getCassandraKeyspace())
              .withTable(options.getCassandraTable())
              .withEntity(entityClass)
              .withCoder(coder);

      String cql = options.getCassandraCql();
      if (cql != null && !cql.trim().isEmpty()) {
        read = read.withQuery(cql);
      }

      String username = options.getCassandraUsername();
      if (username != null && !username.isEmpty()) {
        read = read.withUsername(username).withPassword(options.getCassandraPassword());
      }

      return new CassandraRead<>(read);
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.apply("ReadFromCassandra", delegate);
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

    p.apply(CassandraRead.fromOptions(options, DlqJob.class, SerializableCoder.of(DlqJob.class)))
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
