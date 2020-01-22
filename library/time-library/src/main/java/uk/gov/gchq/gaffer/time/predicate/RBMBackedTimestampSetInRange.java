/*
 * Copyright 2019-2020 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.function.MaskTimestampSetByTimeRange;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;
import uk.gov.gchq.koryphe.util.TimeUnit;

/**
 * Tests whether an RBMBackedTimestampSet contains a value in a given range. If
 * required, the user can specify whether all values within the timestamp set
 * should be tested.
 */
@Since("1.10.0")
@Summary("Tests whether an RBMBackedTimestampSet contains values within a given range")
@JsonPropertyOrder(value = {"class", "startTime", "endTime", "timeUnit", "includeAllTimestamps"})
public class RBMBackedTimestampSetInRange extends KoryphePredicate<RBMBackedTimestampSet> {

    private Number startTime;
    private Number endTime;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private TimeUnit timeUnit = TimeUnit.MILLISECOND;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private Boolean includeAllTimestamps = false;

    public RBMBackedTimestampSetInRange() {
        // required for serialisation
    }

    public RBMBackedTimestampSetInRange(final Number startTime, final Number endTime) {
       this(startTime, endTime, TimeUnit.MILLISECOND, false);
    }

    public RBMBackedTimestampSetInRange(final Number startTime, final Number endTime, final TimeUnit timeUnit) {
        this(startTime, endTime, timeUnit, false);
    }

    public RBMBackedTimestampSetInRange(final Number startTime, final Number endTime, final TimeUnit timeUnit, final Boolean includeAllTimestamps) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.timeUnit = timeUnit;
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

        Long startEpoch = startTime != null ? startTime.longValue() : null;
        Long endEpoch = endTime != null ? endTime.longValue() : null;
        RBMBackedTimestampSet masked = new MaskTimestampSetByTimeRange(startEpoch, endEpoch, timeUnit).apply(rbmBackedTimestampSet);

        if (includeAllTimestamps) {
            return masked.equals(rbmBackedTimestampSet);
        } else {
            return masked.getNumberOfTimestamps() > 0L;
        }
    }

    public Number getStartTime() {
        return startTime;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public void setStartTime(final Number startTime) {
        this.startTime = startTime;
    }

    public RBMBackedTimestampSetInRange startTime(final Number startTime) {
        this.startTime = startTime;
        return this;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public Number getEndTime() {
        return endTime;
    }

    public void setEndTime(final Number endTime) {
        this.endTime = endTime;
    }

    public RBMBackedTimestampSetInRange endTime(final Number endTime) {
        this.endTime = endTime;
        return this;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public RBMBackedTimestampSetInRange timeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        return this;
    }

    public Boolean isIncludeAllTimestamps() {
        return includeAllTimestamps;
    }

    public void setIncludeAllTimestamps(final Boolean includeAllTimestamps) {
        this.includeAllTimestamps = includeAllTimestamps;
    }

    public void setIncludeAllTimestamps() {
        this.includeAllTimestamps = true;
    }

    public RBMBackedTimestampSetInRange includeAllTimestamps() {
        this.includeAllTimestamps = true;
        return this;
    }

    public RBMBackedTimestampSetInRange includeAllTimestamps(final Boolean includeAllTimestamps) {
        this.includeAllTimestamps = includeAllTimestamps;
        return this;
    }
}
