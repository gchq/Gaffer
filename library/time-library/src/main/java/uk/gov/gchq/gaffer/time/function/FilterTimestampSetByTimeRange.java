/*
 * Copyright 2016-2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.time.function;

import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.time.Instant;

/**
 * A {@code FilterTimestampSetByTimeRange} is a {@link uk.gov.gchq.koryphe.function.KorypheFunction} that takes in a
 * {@link RBMBackedTimestampSet} and filters the internal timestamps by a start time and end time.
 */
@Since("1.9.0")
@Summary("Filters a RBMBackedTimestampSet by a time range")
public class FilterTimestampSetByTimeRange extends KorypheFunction<RBMBackedTimestampSet, RBMBackedTimestampSet> {
    long timeRangeStartEpochMilli;
    long timeRangeEndEpochMilli;

    public FilterTimestampSetByTimeRange() {
        // Required for serialisation
    }

    public FilterTimestampSetByTimeRange(final long timeRangeStartEpochMilli, final long timeRangeEndEpochMilli) {
        this.timeRangeStartEpochMilli = timeRangeStartEpochMilli;
        this.timeRangeEndEpochMilli = timeRangeEndEpochMilli;
    }

    @Override
    public RBMBackedTimestampSet apply(final RBMBackedTimestampSet rbmBackedTimestampSet) {
        rbmBackedTimestampSet.filterByTimeRange(Instant.ofEpochMilli(timeRangeStartEpochMilli), Instant.ofEpochMilli(timeRangeEndEpochMilli));
        return rbmBackedTimestampSet;
    }

    public long getTimeRangeStartEpochMilli() {
        return timeRangeStartEpochMilli;
    }

    public void setTimeRangeStartEpochMilli(final long timeRangeStartEpochMilli) {
        this.timeRangeStartEpochMilli = timeRangeStartEpochMilli;
    }

    public long getTimeRangeEndEpochMilli() {
        return timeRangeEndEpochMilli;
    }

    public void setTimeRangeEndEpochMilli(final long timeRangeEndEpochMilli) {
        this.timeRangeEndEpochMilli = timeRangeEndEpochMilli;
    }
}
