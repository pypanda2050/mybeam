package com.example.beam;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class GcsProcessingPipelineTest {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testPipeline() throws Exception {
        // 1. Prepare Input Data
        // Filename should be start_time_in_millis
        // 1698400800000 corresponds to 2023-10-27T10:00:00Z
        File inputFile1 = tmpFolder.newFile("1698400800000.csv");
        List<String> lines1 = Arrays.asList(
                "saga1,nodeA,1698400800000",
                "saga2,nodeA,1698400800000"
        );
        Files.write(inputFile1.toPath(), lines1);

        // Another file in the same hour (if we had one) or different hour
        // 1698404400000 corresponds to 2023-10-27T11:00:00Z
        File inputFile2 = tmpFolder.newFile("1698404400000.csv");
        List<String> lines2 = Arrays.asList(
                "saga3,nodeB,1698404400000"
        );
        Files.write(inputFile2.toPath(), lines2);

        // 2. Define Output Directory
        File outputDir = tmpFolder.newFolder("output");

        // 3. Run Pipeline
        // We use a glob pattern to match the files
        String inputPattern = tmpFolder.getRoot().getAbsolutePath() + "/*.csv";
        
        String[] args = {
                "--input=" + inputPattern,
                "--output=" + outputDir.getAbsolutePath()
        };
        GcsProcessingPipeline.main(args);

        // 4. Verify Output
        // Expected structure:
        // output/nodeA/2023-10-27-10/part-...
        // output/nodeB/2023-10-27-11/part-...

        File nodeADir = new File(outputDir, "nodeA");
        File nodeBDir = new File(outputDir, "nodeB");

        if (!nodeADir.exists()) {
            throw new RuntimeException("Output directory for nodeA not created.");
        }
        if (!nodeBDir.exists()) {
            throw new RuntimeException("Output directory for nodeB not created.");
        }
        
        System.out.println("Test passed: Output directories created.");
    }
}
