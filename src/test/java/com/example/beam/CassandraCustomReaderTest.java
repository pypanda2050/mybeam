package com.example.beam;

import com.example.beam.CassandraCustomReader.DlqJob;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the time-range filter logic in CassandraCustomReader.
 *
 * These tests do not require a running Cassandra instance. They exercise the same
 * SerializableFunction predicate that main() builds from --dlqTsFrom / --dlqTsTo,
 * applied to an in-memory PCollection via Create.of().
 */
@RunWith(JUnit4.class)
public class CassandraCustomReaderTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static DlqJob job(String sagaId, String nodeId, long createTs, long dlqTs) {
    DlqJob j = new DlqJob();
    j.sagaId = sagaId;
    j.nodeId = nodeId;
    j.createTs = createTs;
    j.dlqTs = dlqTs;
    return j;
  }

  private static SerializableFunction<DlqJob, Boolean> timeFilter(Long from, Long to) {
    return job -> (from == null || job.dlqTs >= from) && (to == null || job.dlqTs <= to);
  }

  private static class ExtractSagaId extends SimpleFunction<DlqJob, String> {
    @Override
    public String apply(DlqJob j) {
      return j.sagaId;
    }
  }

  private PCollection<String> toSagaIds(PCollection<DlqJob> jobs) {
    return jobs.apply(MapElements.via(new ExtractSagaId()));
  }

  @Test
  public void testNoBoundsPassesAllRows() {
    List<DlqJob> jobs =
        Arrays.asList(
            job("s1", "n1", 0L, 500L),
            job("s2", "n2", 0L, 1500L),
            job("s3", "n3", 0L, 3000L));

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(null, null))));

    PAssert.that(result).containsInAnyOrder("s1", "s2", "s3");
    p.run().waitUntilFinish();
  }

  @Test
  public void testBothBoundsInclusiveEdges() {
    final Long from = 1000L;
    final Long to = 2000L;

    List<DlqJob> jobs =
        Arrays.asList(
            job("in-mid",    "n", 0L, 1500L), // inside
            job("at-lower",  "n", 0L, 1000L), // exactly at from (inclusive)
            job("at-upper",  "n", 0L, 2000L), // exactly at to (inclusive)
            job("below",     "n", 0L, 999L),  // just below from
            job("above",     "n", 0L, 2001L)  // just above to
        );

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(from, to))));

    PAssert.that(result).containsInAnyOrder("in-mid", "at-lower", "at-upper");
    p.run().waitUntilFinish();
  }

  @Test
  public void testLowerBoundOnlyFiltersBelow() {
    final Long from = 1000L;

    List<DlqJob> jobs =
        Arrays.asList(
            job("pass1", "n", 0L, 1000L),  // at bound (inclusive)
            job("pass2", "n", 0L, 99999L), // well above
            job("drop1", "n", 0L, 999L)    // just below
        );

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(from, null))));

    PAssert.that(result).containsInAnyOrder("pass1", "pass2");
    p.run().waitUntilFinish();
  }

  @Test
  public void testUpperBoundOnlyFiltersAbove() {
    final Long to = 2000L;

    List<DlqJob> jobs =
        Arrays.asList(
            job("pass1", "n", 0L, 1L),    // well below
            job("pass2", "n", 0L, 2000L), // at bound (inclusive)
            job("drop1", "n", 0L, 2001L)  // just above
        );

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(null, to))));

    PAssert.that(result).containsInAnyOrder("pass1", "pass2");
    p.run().waitUntilFinish();
  }

  @Test
  public void testEmptyInputProducesEmptyOutput() {
    PCollection<String> result =
        toSagaIds(
            p.apply(Create.empty(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(1000L, 2000L))));

    PAssert.that(result).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void testAllRowsDroppedWhenNoneInRange() {
    final Long from = 9000L;
    final Long to = 9999L;

    List<DlqJob> jobs =
        Arrays.asList(
            job("s1", "n", 0L, 1000L),
            job("s2", "n", 0L, 2000L),
            job("s3", "n", 0L, 8999L));

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(from, to))));

    PAssert.that(result).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void testSingleRowInRange() {
    final Long from = 500L;
    final Long to = 500L; // exact point range

    List<DlqJob> jobs =
        Arrays.asList(
            job("match", "n", 0L, 500L),
            job("no",    "n", 0L, 499L),
            job("no2",   "n", 0L, 501L));

    PCollection<String> result =
        toSagaIds(
            p.apply(Create.of(jobs).withCoder(SerializableCoder.of(DlqJob.class)))
                .apply(Filter.by(timeFilter(from, to))));

    PAssert.that(result).containsInAnyOrder("match");
    p.run().waitUntilFinish();
  }
}
