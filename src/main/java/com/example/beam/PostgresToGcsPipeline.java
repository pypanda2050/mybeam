package com.example.beam;

import java.sql.ResultSet;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class PostgresToGcsPipeline {

  public interface Options extends PipelineOptions {
    @Description("Postgres JDBC URL")
    @Required
    String getPostgresUrl();

    void setPostgresUrl(String value);

    @Description("Postgres User")
    @Required
    String getPostgresUser();

    void setPostgresUser(String value);

    @Description("Postgres Password")
    @Required
    String getPostgresPassword();

    void setPostgresPassword(String value);

    @Description("JDBC Driver Class")
    @org.apache.beam.sdk.options.Default.String("org.postgresql.Driver")
    String getDriverClass();

    void setDriverClass(String value);

    @Description("Output GCS path prefix")
    @Required
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    // Calculate timestamp for "last 1 hour"
    long oneHourAgo = System.currentTimeMillis() - 3600000;

    PCollection<KV<String, String>> rows =
        p.apply(
            "ReadFromPostgres",
            JdbcIO.<KV<String, String>>read()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                            options.getDriverClass(), options.getPostgresUrl())
                        .withUsername(options.getPostgresUser())
                        .withPassword(options.getPostgresPassword()))
                .withQuery(
                    "SELECT saga_id, node_id, create_ts, dlq_ts FROM dlq_job WHERE dlq_ts >= ?")
                .withStatementPreparator(
                    preparedStatement -> preparedStatement.setLong(1, oneHourAgo))
                .withRowMapper(
                    new JdbcIO.RowMapper<KV<String, String>>() {
                      @Override
                      public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
                        String sagaId = resultSet.getString("saga_id");
                        String nodeId = resultSet.getString("node_id");
                        long createTs = resultSet.getLong("create_ts");
                        long dlqTs = resultSet.getLong("dlq_ts");

                        // Key: dlq_hour (derived from dlq_ts)
                        // Assuming we want YYYY-MM-DD-HH format or similar for the file key
                        // Simple approach: round down to hour
                        long dlqHourMillis = (dlqTs / 3600000) * 3600000;
                        String dlqHour = String.valueOf(dlqHourMillis); // Or format as date string

                        // Value: CSV
                        String csv = String.format("%s,%s,%d,%d", sagaId, nodeId, createTs, dlqTs);

                        return KV.of(dlqHour, csv);
                      }
                    })
                .withCoder(
                    org.apache.beam.sdk.coders.KvCoder.of(
                        StringUtf8Coder.of(), StringUtf8Coder.of())));

    rows.apply(
        "WriteToGCS",
        FileIO.<String, KV<String, String>>writeDynamic()
            .by(KV::getKey)
            .via(Contextful.fn(KV::getValue), TextIO.sink())
            .to(options.getOutput())
            .withNaming(key -> FileIO.Write.defaultNaming("dlq/" + key, ".csv"))
            .withDestinationCoder(StringUtf8Coder.of())
            .withNumShards(1));

    p.run();
  }
}
