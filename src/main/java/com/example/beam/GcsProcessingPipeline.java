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
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

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

    public static class OutputKey implements Serializable {
        private String nodeId;
        private String recordType;
        private String hour;

        public OutputKey() {}

        public OutputKey(String nodeId, String recordType, String hour) {
            this.nodeId = nodeId;
            this.recordType = recordType;
            this.hour = hour;
        }

        public String getNodeId() { return nodeId; }
        public String getRecordType() { return recordType; }
        public String getHour() { return hour; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OutputKey outputKey = (OutputKey) o;
            return Objects.equals(nodeId, outputKey.nodeId) &&
                   Objects.equals(recordType, outputKey.recordType) &&
                   Objects.equals(hour, outputKey.hour);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, recordType, hour);
        }

        @Override
        public String toString() {
            return "OutputKey{nodeId='" + nodeId + "', recordType='" + recordType + "', hour='" + hour + "'}";
        }
    }

    public static class OutputKeyCoder extends org.apache.beam.sdk.coders.AtomicCoder<OutputKey> {
        private static final OutputKeyCoder INSTANCE = new OutputKeyCoder();
        private static final org.apache.beam.sdk.coders.StringUtf8Coder STRING_CODER = org.apache.beam.sdk.coders.StringUtf8Coder.of();

        public static OutputKeyCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(OutputKey value, java.io.OutputStream outStream) throws java.io.IOException {
            STRING_CODER.encode(value.nodeId, outStream);
            STRING_CODER.encode(value.recordType, outStream);
            STRING_CODER.encode(value.hour, outStream);
        }

        @Override
        public OutputKey decode(java.io.InputStream inStream) throws java.io.IOException {
            String nodeId = STRING_CODER.decode(inStream);
            String recordType = STRING_CODER.decode(inStream);
            String hour = STRING_CODER.decode(inStream);
            return new OutputKey(nodeId, recordType, hour);
        }

        @Override
        public void verifyDeterministic() {
            // StringUtf8Coder is deterministic
        }
    }

    public static void main(String[] args) {
        GcsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsOptions.class);
        Pipeline p = Pipeline.create(options);
        
        // Register Coder
        p.getCoderRegistry().registerCoderForClass(OutputKey.class, OutputKeyCoder.of());

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
        PCollection<KV<OutputKey, String>> parsedRecords = groupedFiles.apply("ProcessGroups", ParDo.of(new DoFn<KV<String, Iterable<Metadata>>, KV<OutputKey, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Iterable<Metadata> files = c.element().getValue();

                for (Metadata metadata : files) {
                    try (ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
                         BufferedReader reader = new BufferedReader(new InputStreamReader(Channels.newInputStream(channel)))) {
                        
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.trim().isEmpty()) {
                                continue;
                            }

                            // Assuming CSV: saga_id, node_id, create_ts, record_type
                            String[] parts = line.split(",");
                            if (parts.length < 4) {
                                LOG.warn("Skipping malformed line (missing record_type): " + line);
                                continue;
                            }

                            String sagaId = parts[0].trim();
                            String nodeId = parts[1].trim();
                            String createTs = parts[2].trim();
                            String recordType = parts[3].trim();

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

                            OutputKey key = new OutputKey(nodeId, recordType, recordHour);
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
        parsedRecords.apply("WriteDynamic", FileIO.<OutputKey, KV<OutputKey, String>>writeDynamic()
                .by(KV::getKey)
                .via(Contextful.fn(KV::getValue), TextIO.sink())
                .to(options.getOutput())
                .withNaming(key -> FileIO.Write.defaultNaming(
                        key.getNodeId() + "_" + key.getRecordType() + "_" + key.getHour() + "/part", 
                        ".csv"))
                .withDestinationCoder(OutputKeyCoder.of())
                .withNumShards(1));

        // 5. Aggregation Pipeline
        // Branch from parsedRecords to calculate summary counts
        parsedRecords
            .apply("CountPerKey", org.apache.beam.sdk.transforms.Count.perKey())
            .apply("PivotForSummary", ParDo.of(new DoFn<KV<OutputKey, Long>, KV<String, KV<String, Long>>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    OutputKey key = c.element().getKey();
                    Long count = c.element().getValue();
                    
                    // Grouping key: nodeId + "_" + hour
                    String groupKey = key.getNodeId() + "_" + key.getHour();
                    // Value: recordType, count
                    c.output(KV.of(groupKey, KV.of(key.getRecordType(), count)));
                }
            }))
            .apply("GroupSummary", GroupByKey.create())
            .apply("FormatSummary", ParDo.of(new DoFn<KV<String, Iterable<KV<String, Long>>>, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String groupKey = c.element().getKey(); // nodeId_hour
                    Iterable<KV<String, Long>> counts = c.element().getValue();
                    
                    long countR = 0;
                    long countC = 0;
                    long countD = 0;
                    
                    for (KV<String, Long> item : counts) {
                        String type = item.getKey();
                        Long val = item.getValue();
                        if ("R".equals(type)) countR += val;
                        else if ("C".equals(type)) countC += val;
                        else if ("D".equals(type)) countD += val;
                    }
                    
                    // Parse nodeId from groupKey (nodeId_hour)
                    // Assuming nodeId doesn't contain "_", but hour is yyyy-MM-dd-HH (contains -)
                    // Let's just use the groupKey or try to split.
                    // groupKey = nodeId + "_" + hour
                    // If we want just nodeId in the CSV as per requirement "node_id, count(R)..."
                    // We need to extract it.
                    String nodeId = groupKey.substring(0, groupKey.lastIndexOf('_'));
                    
                    // Output: node_id, countR, countC, countD
                    String csv = nodeId + "," + countR + "," + countC + "," + countD;
                    c.output(csv);
                }
            }))
            .apply("WriteSummary", TextIO.write()
                    .to(options.getOutput() + "/summary_count/summary")
                    .withSuffix(".csv")
                    .withoutSharding());

        p.run();
    }
}
