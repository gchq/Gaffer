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
package uk.gov.gchq.gaffer.time.predicate;

import com.fasterxml.jackson.annotation.JsonInclude;

import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.function.MaskTimestampSetByTimeRange;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.time.Instant;

/**
 * Test whether an RBMBackedTimestampSet contains a value in a given range. If
 * required, the user can specify whether all values within the timestamp set
 * should be tested.
 */
public class RBMBackedTimestampSetInRange extends KoryphePredicate<RBMBackedTimestampSet> {

    private Instant start;
    private Instant end;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Boolean includeAllTimestamps = false;

    public RBMBackedTimestampSetInRange() {
        // required for serialisation
    }

    public RBMBackedTimestampSetInRange(final Instant start, final Instant end) {
       this(start, end, false);
    }

    public RBMBackedTimestampSetInRange(final Instant start, final Instant end, final Boolean includeAllTimestamps) {
        this.start = start;
        this.end = end;
        this.includeAllTimestamps = includeAllTimestamps;
    }


    @Override
    public boolean test(final RBMBackedTimestampSet rbmBackedTimestampSet) {
        if (rbmBackedTimestampSet == null) {
            throw new IllegalArgumentException("TimestampSet cannot be null");
        }
        if (rbmBackedTimestampSet.getNumberOfTimestamps() == 0) {
            throw new IllegalArgumentException("TimestampSet must contain at least one value");
        }

        Long startEpoch = start != null ? start.toEpochMilli() : null;
        Long endEpoch = end != null ? end.toEpochMilli() : null;
        RBMBackedTimestampSet masked = new MaskTimestampSetByTimeRange(startEpoch, endEpoch).apply(rbmBackedTimestampSet);

        if (includeAllTimestamps) {
            return masked.equals(rbmBackedTimestampSet);
        } else {
            return masked.getNumberOfTimestamps() > 0L;
        }
    }

    public Instant getStart() {
        return start;
    }

    public RBMBackedTimestampSetInRange start(final Instant start) {
        this.start = start;
        return this;
    }

    public void setStart(final Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public RBMBackedTimestampSetInRange end(final Instant end) {
        this.end = end;
        return this;
    }

    public void setEnd(final Instant end) {
        this.end = end;
    }

    public Boolean getIncludeAllTimestamps() {
        return includeAllTimestamps;
    }

    public RBMBackedTimestampSetInRange includeAllTimestamps() {
        this.includeAllTimestamps = true;
        return this;
    }

    public RBMBackedTimestampSetInRange includeAllTimestamps(final Boolean includeAllTimestamps) {
        this.includeAllTimestamps = includeAllTimestamps;
        return this;
    }

    public void setIncludeAllTimestamps(final Boolean includeAllTimestamps) {
        this.includeAllTimestamps = includeAllTimestamps;
    }

    public void setIncludeAllTimestamps() {
        this.includeAllTimestamps = true;
    }
}
