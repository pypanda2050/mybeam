package com.example.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class GcsProcessingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(GcsProcessingPipeline.class);

    public interface GcsOptions extends PipelineOptions {
        @Description("Input GCS file pattern prefix")
        @Required
        String getInput();
        void setInput(String value);

        @Description("Output GCS directory")
        @Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        GcsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsOptions.class);
        Pipeline p = Pipeline.create(options);

        // 1. Match files
        PCollection<Metadata> matches = p.apply("MatchFiles", FileIO.match().filepattern(options.getInput()));

        // 2. Group by Hour (extracted from filename)
        PCollection<KV<String, Iterable<Metadata>>> groupedFiles = matches
            .apply("ExtractHourKey", ParDo.of(new DoFn<Metadata, KV<String, Metadata>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Metadata metadata = c.element();
                    ResourceId resourceId = metadata.resourceId();
                    String filename = resourceId.getFilename();
                    
                    // Filename is start_time_in_millis (possibly with extension)
                    // Remove extension if present
                    if (filename.contains(".")) {
                        filename = filename.substring(0, filename.lastIndexOf('.'));
                    }

                    String hour = "unknown";
                    try {
                        long epochMillis = Long.parseLong(filename);
                        Instant instant = Instant.ofEpochMilli(epochMillis);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                                .withZone(ZoneId.of("UTC"));
                        hour = formatter.format(instant);
                    } catch (NumberFormatException e) {
                        LOG.warn("Could not parse filename as timestamp: " + filename);
                        hour = "invalid_ts";
                    }
                    
                    c.output(KV.of(hour, metadata));
                }
            }))
            .apply("GroupByHour", GroupByKey.create());

        // 3. Read and Process Files in Group
        PCollection<KV<String, String>> parsedRecords = groupedFiles.apply("ProcessGroups", ParDo.of(new DoFn<KV<String, Iterable<Metadata>>, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String hourKey = c.element().getKey();
                Iterable<Metadata> files = c.element().getValue();

                for (Metadata metadata : files) {
                    try (ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
                         BufferedReader reader = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel)))) {
                        
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.trim().isEmpty()) {
                                continue;
                            }

                            // Assuming CSV: saga_id, node_id, create_ts
                            String[] parts = line.split(",");
                            if (parts.length < 3) {
                                LOG.warn("Skipping malformed line: " + line);
                                continue;
                            }

                            String sagaId = parts[0].trim();
                            String nodeId = parts[1].trim();
                            String createTs = parts[2].trim();

                            // We still need to parse createTs for the output key (node_id + hour)
                            // Even though we grouped files by hour, the record's create_ts might differ slightly?
                            // Or we strictly use the file's hour? 
                            // The requirement says: "keyed by node id and hour"
                            // And "read all files from GCS in same hour in same group".
                            // It's safer to use the record's own timestamp for the output key to be precise,
                            // or if the file implies the hour for all records, we could use 'hourKey'.
                            // Let's stick to parsing the record's createTs as per previous logic to be safe.
                            
                            String recordHour = "unknown";
                            try {
                                long epochMillis = Long.parseLong(createTs);
                                Instant instant = Instant.ofEpochMilli(epochMillis);
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
                                        .withZone(ZoneId.of("UTC"));
                                recordHour = formatter.format(instant);
                            } catch (NumberFormatException e) {
                                LOG.warn("Could not parse timestamp as long: " + createTs);
                                recordHour = "invalid_ts";
                            }

                            String key = nodeId + "/" + recordHour;
                            String value = sagaId + "," + createTs;
                            
                            c.output(KV.of(key, value));
                        }
                    } catch (Exception e) {
                        LOG.error("Error reading file: " + metadata.resourceId(), e);
                    }
                }
            }
        }));

        // 4. Write Dynamic
        parsedRecords.apply("WriteDynamic", FileIO.<String, KV<String, String>>writeDynamic()
                .by(KV::getKey)
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .to(options.getOutput())
                .withNaming(key -> FileIO.Write.defaultNaming(key + "/part", ".csv"))
                .withDestinationCoder(org.apache.beam.sdk.coders.StringUtf8Coder.of())
                .withNumShards(1));

        p.run();
    }
}
