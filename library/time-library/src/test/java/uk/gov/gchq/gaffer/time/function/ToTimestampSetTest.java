package uk.gov.gchq.gaffer.time.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.BoundedTimestampSet;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.TimestampSet;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;


class ToTimestampSetTest extends FunctionTest<ToTimestampSet> {
    private static Long TEST_TIMESTAMPS = Instant.now().toEpochMilli();

    @Test
    void shouldCreateEmptySetWhenNull() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        TimestampSet result = toTimestampSet.apply(null);

        // Then
        TimestampSet expected = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .build();

        assertEquals(expected, result);

    }

    @Test
    void shouldCreateBoundedSet() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, 10);
        // When
        TimestampSet result = toTimestampSet.apply(TEST_TIMESTAMPS);

        // Then
        TimestampSet expected = new BoundedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .maxSize(10)
                .build();
        expected.add(Instant.ofEpochMilli(TEST_TIMESTAMPS));

        assertEquals(expected, result);

    }

    @Test
    void shouldCreateNonBoundedSet() {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        TimestampSet result = toTimestampSet.apply(TEST_TIMESTAMPS);

        // Then
        TimestampSet expected = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.DAY)
                .build();
        expected.add(Instant.ofEpochMilli(TEST_TIMESTAMPS));

        assertEquals(expected, result);

    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Long.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TimestampSet.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToTimestampSet toTimestampSet =
                new ToTimestampSet(CommonTimeUtil.TimeBucket.DAY, false);
        // When
        final String json = new String(JSONSerialiser.serialise(toTimestampSet));
        ToTimestampSet deserialisedToTimestampSet = JSONSerialiser.deserialise(json, ToTimestampSet.class);
        // Then
        assertEquals(toTimestampSet, deserialisedToTimestampSet);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimestampSet\",\"bucket\":\"DAY\",\"millisCorrection\":1}", json );
    }

    @Override
    protected ToTimestampSet getInstance() {
        return new ToTimestampSet();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}