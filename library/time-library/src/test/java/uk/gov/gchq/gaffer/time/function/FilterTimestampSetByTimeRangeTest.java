package uk.gov.gchq.gaffer.time.function;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class FilterTimestampSetByTimeRangeTest extends FunctionTest {

    private Instant instant;
    private FilterTimestampSetByTimeRange filterTimestampSetByTimeRange = new FilterTimestampSetByTimeRange();

    @Before
    public void setup() {
        instant = Instant.now();
    }

    @Test
    public void filtersTimestampSet() {
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        timestampSet.add(instant);
        timestampSet.add(instant.plus(Duration.ofDays(100L)));
        timestampSet.add(instant.plus(Duration.ofDays(200L)));
        timestampSet.add(instant.plus(Duration.ofDays(300L)));

        filterTimestampSetByTimeRange.setTimeRangeStartEpochMilli(instant.plus(Duration.ofDays(100)).toEpochMilli());
        filterTimestampSetByTimeRange.setTimeRangeEndEpochMilli(instant.plus(Duration.ofDays(250)).toEpochMilli());

        final RBMBackedTimestampSet expectedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        expectedTimestampSet.add(instant.plus(Duration.ofDays(100L)));
        expectedTimestampSet.add(instant.plus(Duration.ofDays(200L)));

        final RBMBackedTimestampSet actualTimestampSet = filterTimestampSetByTimeRange.apply(timestampSet);

        assertEquals(expectedTimestampSet, actualTimestampSet);
    }

    @Override
    protected Function getInstance() {
        return filterTimestampSetByTimeRange;
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return FilterTimestampSetByTimeRange.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final FilterTimestampSetByTimeRange filterTimestampSetByTimeRange = new FilterTimestampSetByTimeRange(1l,2l);

        // When
        final String json = new String(JSONSerialiser.serialise(filterTimestampSetByTimeRange));
        FilterTimestampSetByTimeRange deserialisedFilterTimestampSetByTimeRange = JSONSerialiser.deserialise(json, FilterTimestampSetByTimeRange.class);

        // Then
        assertEquals(filterTimestampSetByTimeRange, deserialisedFilterTimestampSetByTimeRange);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.FilterTimestampSetByTimeRange\",\"timeRangeStartEpochMilli\":1,\"timeRangeEndEpochMilli\":2}"
                , json);
    }
}