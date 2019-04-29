/*
 * Copyright 2019 Crown Copyright
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
import uk.gov.gchq.koryphe.util.TimeUnit;

/**
 * A {@code MaskTimestampSetByTimeRange} is a {@link uk.gov.gchq.koryphe.function.KorypheFunction} that takes in a
 * {@link RBMBackedTimestampSet} and filters the internal timestamps by a start time and end time.
 */
@Since("1.9.0")
@Summary("Masks a RBMBackedTimestampSet by a time range")
public class MaskTimestampSetByTimeRange extends KorypheFunction<RBMBackedTimestampSet, RBMBackedTimestampSet> {
    private Long startTime;
    private Long endTime;
    private TimeUnit timeUnit;

    public MaskTimestampSetByTimeRange() {
        this(null, null);
    }

    public MaskTimestampSetByTimeRange(final Long startTime, final Long endTime) {
        this(startTime, endTime, TimeUnit.MILLISECOND);
    }

    public MaskTimestampSetByTimeRange(final Long startTime, final Long endTime, final TimeUnit timeUnit) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeUnit = timeUnit;
    }

    @Override
    public RBMBackedTimestampSet apply(final RBMBackedTimestampSet rbmBackedTimestampSet) {
        RBMBackedTimestampSet cloned = rbmBackedTimestampSet.getShallowClone();
        cloned.applyTimeRangeMask(timeUnit.asMilliSeconds(startTime), timeUnit.asMilliSeconds(endTime));
        return cloned;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(final Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(final Long endTime) {
        this.endTime = endTime;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public static final class Builder {
        private TimeUnit timeUnit = TimeUnit.MILLISECOND;
        private Long startTime;
        private Long endTime;

        public MaskTimestampSetByTimeRange build() {
            return new MaskTimestampSetByTimeRange(startTime, endTime, timeUnit);
        }

        public Builder startTime(final Long startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(final Long endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder timeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }
    }
}
