package com.example.beam;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.example.beam.CassandraCustomReader.CassandraOptions;
import com.example.beam.CassandraCustomReader.DlqJob;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * A Cassandra reader that pushes the {@code dlq_ts} time-range filter into the CQL query at
 * execution time, alongside the per-split token-range predicates, without requiring a secondary
 * index.
 *
 * <h3>Why the other approaches fall short</h3>
 *
 * <ul>
 *   <li>{@link CassandraCustomReader.CassandraRead} reads all rows per token split and discards
 *       out-of-range records in a Beam {@code Filter} step — no server-side pushdown.
 *   <li>{@link CustomCassandraReaderWithFilteringCQL.CassandraReadWithCql} passes the filter via
 *       {@code CassandraIO.withQuery()}, but {@code CassandraIO.ReadFn.buildInitialQuery()} appends
 *       token predicates directly after the custom query string. This forces the filter to be
 *       index-backed (SASI) because {@code ALLOW FILTERING} cannot appear before the token
 *       predicates without producing invalid CQL.
 * </ul>
 *
 * <h3>This implementation's approach</h3>
 *
 * <p>We bypass {@code CassandraIO} entirely for query execution and own the full CQL construction:
 *
 * <pre>
 *   SELECT * FROM my_keyspace.dlq_job
 *     WHERE dlq_ts >= 1000 AND dlq_ts <= 2000          &lt;-- user filter
 *     AND token(saga_id) >= -9223372036854775808        &lt;-- token range (per split)
 *     AND token(saga_id) &lt;  -4611686018427387903
 *     ALLOW FILTERING                                   &lt;-- always last — valid CQL
 * </pre>
 *
 * <p>When no filter is set, the query omits both the column predicates and {@code ALLOW FILTERING},
 * relying solely on token-range scans for a full-table read.
 *
 * <h3>Why {@code ALLOW FILTERING} is acceptable here</h3>
 *
 * <p>Each split scans only its token-range slice of the ring — roughly {@code 1/numSplits} of the
 * data. {@code ALLOW FILTERING} therefore performs a bounded sequential scan within that slice, not
 * a full-table scan. Increasing {@code --cassandraMinSplits} reduces the per-split scan size.
 *
 * <h3>No secondary index required</h3>
 *
 * <p>Because {@code ALLOW FILTERING} is at the end of the CQL, Cassandra accepts the range
 * predicate on {@code dlq_ts} without any secondary index (regular, SASI, or otherwise).
 *
 * <h3>Pipeline graph</h3>
 *
 * <ol>
 *   <li>{@code Create.of(spec)} — emits a single {@link ReadSpec} element.
 *   <li>{@code ParDo(SplitFn)} — connects to Cassandra once to read ring topology; emits one
 *       {@link RangedQuery} per token-range sub-split.
 *   <li>{@code Reshuffle.viaRandomKey()} — breaks stage fusion, ensuring the runner distributes
 *       {@link RangedQuery} elements across worker nodes.
 *   <li>{@code ParDo(ReadFn)} — each worker opens a long-lived Cassandra connection via
 *       {@code @Setup}; per element it builds the full per-split CQL and streams rows back as
 *       entity objects via the DataStax {@code MappingManager}.
 * </ol>
 *
 * <h3>Partitioner support</h3>
 *
 * <p>Only {@code Murmur3Partitioner} (token values are {@code long}) is supported. Clusters using
 * {@code RandomPartitioner} or {@code ByteOrderedPartitioner} will receive a descriptive
 * {@link IllegalStateException} at split time.
 */
public class CassandraReaderWithFilteringQuery {

  // ── ReadSpec: full configuration, passed as a single Beam element to SplitFn ────────────────

  /**
   * Self-contained description of the read operation. Passed as the sole element of the first
   * {@code PCollection} so that {@link SplitFn} can be stateless (all configuration flows through
   * the element, not through constructor fields that would require re-serialisation).
   */
  static final class ReadSpec implements Serializable {
    final String hosts;
    final int port;
    final String username;          // null → no auth
    final String password;          // null → no auth
    final String keyspace;
    final String table;
    final String partitionKeyColumn; // e.g. "saga_id"
    final String filterExpression;   // null → no column filter; e.g. "dlq_ts >= 1000 AND dlq_ts <= 2000"
    final int minSplits;             // 0 → one sub-split per vnode

    ReadSpec(
        String hosts,
        int port,
        String username,
        String password,
        String keyspace,
        String table,
        String partitionKeyColumn,
        String filterExpression,
        int minSplits) {
      this.hosts = hosts;
      this.port = port;
      this.username = username;
      this.password = password;
      this.keyspace = keyspace;
      this.table = table;
      this.partitionKeyColumn = partitionKeyColumn;
      this.filterExpression = filterExpression;
      this.minSplits = minSplits;
    }
  }

  // ── RangedQuery: a single token-range slice to execute ──────────────────────────────────────

  /**
   * Lightweight per-split descriptor. Only carries what changes between splits: the token-range
   * bounds. The filter expression is included once (same for all splits) so that each split is
   * fully self-describing for the {@link ReadFn}.
   */
  static final class RangedQuery implements Serializable {
    final String filterExpression; // null → no column filter
    final long tokenStart;         // inclusive lower bound
    final long tokenEnd;           // exclusive upper bound

    RangedQuery(String filterExpression, long tokenStart, long tokenEnd) {
      this.filterExpression = filterExpression;
      this.tokenStart = tokenStart;
      this.tokenEnd = tokenEnd;
    }
  }

  // ── SplitFn: ring-topology split generation ──────────────────────────────────────────────────

  /**
   * Connects to Cassandra once (within {@code @ProcessElement}, no constructor state) to read
   * ring topology and emits one {@link RangedQuery} per token-range sub-split.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Fetch all vnode token ranges via {@code cluster.getMetadata().getTokenRanges()}.
   *   <li>Compute {@code splitsPerVnode = ceil(minSplits / vnodeCount)}; minimum 1.
   *   <li>Unwrap any ring-wrapping ranges into non-wrapping sub-ranges.
   *   <li>Sub-divide each non-wrapping range into {@code splitsPerVnode} equal pieces.
   *   <li>Emit a {@link RangedQuery} for each piece.
   * </ol>
   */
  static class SplitFn extends DoFn<ReadSpec, RangedQuery> {

    @ProcessElement
    public void processElement(@Element ReadSpec spec, OutputReceiver<RangedQuery> out) {
      Cluster cluster = null;
      try {
        cluster = buildCluster(spec.hosts, spec.port, spec.username, spec.password);
        Metadata metadata = cluster.getMetadata();
        Set<TokenRange> vnodeRanges = metadata.getTokenRanges();

        // 0 → use vnode count as-is (one sub-split per vnode, same as CassandraIO default).
        int effectiveMinSplits = spec.minSplits > 0 ? spec.minSplits : vnodeRanges.size();
        int splitsPerVnode =
            (int) Math.max(1, Math.ceil((double) effectiveMinSplits / vnodeRanges.size()));

        for (TokenRange vnodeRange : vnodeRanges) {
          // unwrap() decomposes a ring-wrapping range (start > end numerically) into at most
          // two non-wrapping ranges so that splitEvenly() produces clean sub-ranges.
          for (TokenRange nonWrapping : vnodeRange.unwrap()) {
            for (TokenRange subRange : nonWrapping.splitEvenly(splitsPerVnode)) {
              long start = murmur3Value(subRange.getStart());
              long end   = murmur3Value(subRange.getEnd());
              out.output(new RangedQuery(spec.filterExpression, start, end));
            }
          }
        }
      } finally {
        if (cluster != null) cluster.close();
      }
    }

    /**
     * Extracts the numeric token value for a {@code Murmur3Partitioner} token.
     *
     * @throws IllegalStateException if the cluster uses a non-Murmur3 partitioner
     */
    private static long murmur3Value(Token token) {
      Object v = token.getValue();
      if (v instanceof Long) {
        return (Long) v;
      }
      throw new IllegalStateException(
          "CassandraReaderWithFilteringQuery only supports Murmur3Partitioner (Long token values)."
              + " Got token type "
              + v.getClass().getName()
              + ". For RandomPartitioner or ByteOrderedPartitioner use CassandraIO directly.");
    }
  }

  // ── ReadFn: per-split CQL execution ─────────────────────────────────────────────────────────

  /**
   * Long-lived per-worker Cassandra connection. {@code @Setup} opens the connection once;
   * {@code @ProcessElement} executes a fresh CQL query for each assigned {@link RangedQuery}
   * and streams entity objects to the output receiver.
   *
   * <p>The CQL built for each split:
   *
   * <pre>
   *   -- with filter:
   *   SELECT * FROM ks.tbl
   *     WHERE &lt;filterExpression&gt;
   *     AND token(pk) >= &lt;tokenStart&gt; AND token(pk) &lt; &lt;tokenEnd&gt;
   *     ALLOW FILTERING
   *
   *   -- without filter (token-only scan):
   *   SELECT * FROM ks.tbl
   *     WHERE token(pk) >= &lt;tokenStart&gt; AND token(pk) &lt; &lt;tokenEnd&gt;
   * </pre>
   */
  static class ReadFn<T> extends DoFn<RangedQuery, T> {

    private final String hosts;
    private final int port;
    private final String username;
    private final String password;
    private final String keyspace;
    private final String table;
    private final String partitionKeyColumn;
    private final Class<T> entityClass;

    // Transient: not serialized; re-created on each worker via @Setup.
    private transient Cluster cluster;
    private transient Session session;
    private transient Mapper<T> mapper;

    ReadFn(
        String hosts,
        int port,
        String username,
        String password,
        String keyspace,
        String table,
        String partitionKeyColumn,
        Class<T> entityClass) {
      this.hosts = hosts;
      this.port = port;
      this.username = username;
      this.password = password;
      this.keyspace = keyspace;
      this.table = table;
      this.partitionKeyColumn = partitionKeyColumn;
      this.entityClass = entityClass;
    }

    @Setup
    public void setup() {
      cluster = buildCluster(hosts, port, username, password);
      session = cluster.connect(keyspace);
      mapper = new MappingManager(session).mapper(entityClass);
    }

    @Teardown
    public void teardown() {
      try {
        if (session != null) {
          session.close();
          session = null;
        }
      } catch (Exception ignored) {
      }
      try {
        if (cluster != null) {
          cluster.close();
          cluster = null;
        }
      } catch (Exception ignored) {
      }
    }

    @ProcessElement
    public void processElement(@Element RangedQuery query, OutputReceiver<T> out) {
      String cql =
          buildCql(keyspace, table, partitionKeyColumn,
              query.filterExpression, query.tokenStart, query.tokenEnd);
      mapper.map(session.execute(cql)).forEach(out::output);
    }
  }

  // ── CQL construction (package-private for unit testing) ─────────────────────────────────────

  /**
   * Builds the per-split CQL statement.
   *
   * <p>When {@code filterExpression} is non-null the column predicate is placed before the token
   * range, and {@code ALLOW FILTERING} is appended last — making it valid CQL even though
   * {@code dlq_ts} has no secondary index:
   *
   * <pre>
   *   SELECT * FROM ks.tbl
   *     WHERE dlq_ts >= 1000 AND dlq_ts <= 2000
   *     AND token(saga_id) >= -9223372036854775808
   *     AND token(saga_id) &lt;  -4611686018427387903
   *     ALLOW FILTERING
   * </pre>
   *
   * <p>When {@code filterExpression} is {@code null}, the token-only form is used and
   * {@code ALLOW FILTERING} is omitted (token predicates never require it):
   *
   * <pre>
   *   SELECT * FROM ks.tbl
   *     WHERE token(saga_id) >= -9223372036854775808
   *     AND token(saga_id) &lt; -4611686018427387903
   * </pre>
   *
   * @param keyspace          Cassandra keyspace name
   * @param table             Cassandra table name
   * @param partitionKeyColumn name of the partition-key column (for the {@code token()} call)
   * @param filterExpression  column-level WHERE predicates, or {@code null}
   * @param tokenStart        inclusive lower bound of the token range
   * @param tokenEnd          exclusive upper bound of the token range
   */
  static String buildCql(
      String keyspace,
      String table,
      String partitionKeyColumn,
      String filterExpression,
      long tokenStart,
      long tokenEnd) {

    boolean hasFilter = filterExpression != null && !filterExpression.isEmpty();

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(keyspace).append(".").append(table).append(" WHERE ");

    if (hasFilter) {
      sb.append(filterExpression).append(" AND ");
    }

    sb.append("token(").append(partitionKeyColumn).append(") >= ").append(tokenStart)
        .append(" AND token(").append(partitionKeyColumn).append(") < ").append(tokenEnd);

    if (hasFilter) {
      sb.append(" ALLOW FILTERING");
    }

    return sb.toString();
  }

  /**
   * Builds the filter expression (the WHERE-clause predicates only, without the {@code WHERE}
   * keyword) from pipeline options. Returns {@code null} when neither bound is set.
   *
   * <ul>
   *   <li>{@code dlqTsFrom=1000, dlqTsTo=2000} → {@code "dlq_ts >= 1000 AND dlq_ts <= 2000"}
   *   <li>{@code dlqTsFrom=1000} → {@code "dlq_ts >= 1000"}
   *   <li>{@code dlqTsTo=2000}   → {@code "dlq_ts <= 2000"}
   *   <li>neither set            → {@code null}
   * </ul>
   */
  static String buildFilterExpression(CassandraOptions options) {
    Long from = options.getDlqTsFrom();
    Long to   = options.getDlqTsTo();
    if (from == null && to == null) {
      return null;
    }
    if (from != null && to != null) {
      return String.format("dlq_ts >= %d AND dlq_ts <= %d", from, to);
    }
    if (from != null) {
      return String.format("dlq_ts >= %d", from);
    }
    return String.format("dlq_ts <= %d", to);
  }

  /**
   * Derives the partition-key column name from the entity class by inspecting
   * {@link PartitionKey @PartitionKey} and {@link Column @Column} annotations.
   *
   * @throws IllegalArgumentException if no {@code @PartitionKey}-annotated field is found
   */
  static String resolvePartitionKeyColumn(Class<?> entityClass) {
    for (Field field : entityClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(PartitionKey.class)) {
        Column col = field.getAnnotation(Column.class);
        if (col != null && !col.name().isEmpty()) {
          return col.name();
        }
        return field.getName(); // fall back to Java field name
      }
    }
    throw new IllegalArgumentException(
        "No @PartitionKey-annotated field found in " + entityClass.getName()
            + ". The entity class must annotate exactly one field with "
            + "@com.datastax.driver.mapping.annotations.PartitionKey.");
  }

  // ── Shared cluster builder ───────────────────────────────────────────────────────────────────

  private static Cluster buildCluster(String hosts, int port, String username, String password) {
    Cluster.Builder builder =
        Cluster.builder()
            .addContactPoints(hosts.trim().split("\\s*,\\s*"))
            .withPort(port);
    if (username != null && !username.isEmpty()) {
      builder.withCredentials(username, password);
    }
    return builder.build();
  }

  // ── Public PTransform ────────────────────────────────────────────────────────────────────────

  /**
   * PTransform that reads from Cassandra by building and executing per-split CQL queries
   * that embed both the {@code dlq_ts} range filter and the token-range predicates, with
   * {@code ALLOW FILTERING} always appearing last.
   *
   * <p>No secondary index is required on {@code dlq_ts}. The performance cost of
   * {@code ALLOW FILTERING} is bounded per split: each split scans only its token-range slice of
   * the ring, so the effective scan size is {@code total_rows / numSplits}.
   */
  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    private final ReadSpec spec;
    private final Class<T> entityClass;
    private final Coder<T> coder;

    private Read(ReadSpec spec, Class<T> entityClass, Coder<T> coder) {
      this.spec = spec;
      this.entityClass = entityClass;
      this.coder = coder;
    }

    /**
     * Builds a {@code Read} transform from pipeline options.
     *
     * <p>The partition-key column is resolved automatically from the entity class's
     * {@code @PartitionKey} annotation; it does not need to be specified in the options.
     */
    public static <T> Read<T> fromOptions(
        CassandraOptions options, Class<T> entityClass, Coder<T> coder) {

      String filterExpression = buildFilterExpression(options);
      String partitionKeyColumn = resolvePartitionKeyColumn(entityClass);
      int minSplits = options.getCassandraMinSplits() != null ? options.getCassandraMinSplits() : 0;

      ReadSpec spec =
          new ReadSpec(
              options.getCassandraHosts(),
              options.getCassandraPort(),
              options.getCassandraUsername(),
              options.getCassandraPassword(),
              options.getCassandraKeyspace(),
              options.getCassandraTable(),
              partitionKeyColumn,
              filterExpression,
              minSplits);

      return new Read<>(spec, entityClass, coder);
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      // Stage 1 — single ReadSpec element triggers SplitFn.
      PCollection<ReadSpec> specCol =
          input.apply(
              "CreateReadSpec",
              Create.of(spec).withCoder(SerializableCoder.of(ReadSpec.class)));

      // Stage 2 — SplitFn reads ring topology and emits one RangedQuery per split.
      PCollection<RangedQuery> splits =
          specCol
              .apply("SplitIntoTokenRanges", ParDo.of(new SplitFn()))
              .setCoder(SerializableCoder.of(RangedQuery.class));

      // Stage 3 — Reshuffle breaks stage fusion, enabling the runner to distribute
      //           RangedQuery elements across different worker nodes/threads.
      PCollection<RangedQuery> distributed =
          splits.apply("ReshuffleForDistribution", Reshuffle.viaRandomKey());

      // Stage 4 — ReadFn opens a Cassandra connection per worker (@Setup) and executes
      //           the per-split CQL for each assigned RangedQuery element.
      PCollection<T> rows =
          distributed.apply("ReadFromCassandra", ParDo.of(
              new ReadFn<>(
                  spec.hosts,
                  spec.port,
                  spec.username,
                  spec.password,
                  spec.keyspace,
                  spec.table,
                  spec.partitionKeyColumn,
                  entityClass)));
      rows.setCoder(coder);
      return rows;
    }
  }

  // ── main() ───────────────────────────────────────────────────────────────────────────────────

  public static void main(String[] args) {
    CassandraOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CassandraOptions.class);

    Pipeline p = Pipeline.create(options);

    p.apply(Read.fromOptions(options, DlqJob.class, SerializableCoder.of(DlqJob.class)))
        .apply(
            "FormatCsv",
            MapElements.via(
                new SimpleFunction<DlqJob, String>() {
                  @Override
                  public String apply(DlqJob job) {
                    return job.sagaId + "," + job.nodeId + "," + job.createTs + "," + job.dlqTs;
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
