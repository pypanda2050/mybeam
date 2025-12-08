package com.example.beam;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GcsProcessingPipelineTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testPipeline() throws Exception {
    // 1. Prepare Input Data (DLQ format: sagaIdm,nodeId,dlqTs)
    // Filename should be start_time_in_millis
    // 1698400800000 corresponds to 2023-10-27T10:00:00Z
    File inputFile1 = tmpFolder.newFile("1698400800000.csv");
    List<String> lines1 = Arrays.asList("saga1,nodeA,1698400800000", "saga2,nodeA,1698400800000");
    Files.write(inputFile1.toPath(), lines1);

    // Another file
    // 1698404400000 corresponds to 2023-10-27T11:00:00Z
    File inputFile2 = tmpFolder.newFile("1698404400000.csv");
    List<String> lines2 = Arrays.asList("saga4,nodeB,1698404400000", "saga5,nodeB,1698404400000");
    Files.write(inputFile2.toPath(), lines2);

    // 2. Define Output Directory
    File outputDir = tmpFolder.newFolder("output");

    // 3. Run Pipeline
    String inputPattern = tmpFolder.getRoot().getAbsolutePath() + "/*.csv";

    String[] args = {"--input=" + inputPattern, "--output=" + outputDir.getAbsolutePath()};
    GcsProcessingPipeline.main(args);

    // 4. Verify Output
    // Check dynamic write output
    File nodeADir = new File(outputDir, "nodeA_D_2023-10-27-10");
    if (!nodeADir.exists()) {
      throw new RuntimeException("Output directory for nodeA_D not created.");
    }

    File nodeBDir = new File(outputDir, "nodeB_D_2023-10-27-11");
    if (!nodeBDir.exists()) {
      throw new RuntimeException("Output directory for nodeB_D not created.");
    }

    // Check summary output
    File summaryDir = new File(outputDir, "summary_count");
    if (!summaryDir.exists()) {
      throw new RuntimeException("Summary directory not created.");
    }

    // Find summary file
    File[] summaryFiles =
        summaryDir.listFiles((dir, name) -> name.startsWith("summary") && name.endsWith(".csv"));
    if (summaryFiles == null || summaryFiles.length == 0) {
      throw new RuntimeException("Summary file not found.");
    }

    // Read summary content
    // Expected for DLQ (all 'D'):
    // nodeA, 0, 0, 2
    // nodeB, 0, 0, 2

    List<String> summaryLines = Files.readAllLines(summaryFiles[0].toPath());
    boolean foundNodeA = false;
    boolean foundNodeB = false;

    for (String line : summaryLines) {
      if (line.contains("nodeA,0,0,2")) foundNodeA = true;
      if (line.contains("nodeB,0,0,2")) foundNodeB = true;
    }

    if (!foundNodeA)
      throw new RuntimeException("Summary for nodeA incorrect or missing: " + summaryLines);
    if (!foundNodeB)
      throw new RuntimeException("Summary for nodeB incorrect or missing: " + summaryLines);

    System.out.println("Test passed: Output directories and summary created.");
  }

  @Test
  public void testPipelineDeduplication() throws Exception {
    // 1. Prepare Input Data with Duplicates (DLQ format)
    File inputFile = tmpFolder.newFile("1698400800000_dedup.csv");
    List<String> lines =
        Arrays.asList(
            "saga1,nodeA,1698400800000",
            "saga1,nodeA,1698400800000", // Duplicate
            "saga2,nodeA,1698400800000");
    Files.write(inputFile.toPath(), lines);

    // 2. Define Output Directory
    File outputDir = tmpFolder.newFolder("output_dedup");

    // 3. Run Pipeline
    String inputPattern = inputFile.getAbsolutePath();

    String[] args = {"--input=" + inputPattern, "--output=" + outputDir.getAbsolutePath()};
    GcsProcessingPipeline.main(args);

    // 4. Verify Output
    // Check output file content
    File nodeADDir = new File(outputDir, "nodeA_D_2023-10-27-10");
    File[] outputFiles =
        nodeADDir.listFiles((dir, name) -> name.startsWith("part") && name.endsWith(".csv"));

    if (outputFiles == null || outputFiles.length == 0) {
      throw new RuntimeException("Output file not found.");
    }

    List<String> outputLines = Files.readAllLines(outputFiles[0].toPath());
    // Should contain only 1 line for saga1
    long countSaga1 = outputLines.stream().filter(l -> l.contains("saga1")).count();

    if (countSaga1 != 1) {
      throw new RuntimeException("Duplicate record found. Expected 1 saga1, found " + countSaga1);
    }

    System.out.println("Test passed: Duplicates removed.");
  }
}
