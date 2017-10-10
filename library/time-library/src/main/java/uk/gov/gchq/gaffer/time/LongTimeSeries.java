/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.time;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This is a time series where the values are {@link Long}s. When the time series
 * is created, a {@link TimeBucket} is specified. When timestamps are added, they
 * are rounded to the nearest bucket, e.g. if a {@link TimeBucket} of
 * <code>MINUTE</code> is specified, and a timestamp of January 1st 2017, 12:34:56
 * is added then the seconds are removed so that the value is associated to
 * 12:34.
 */
public class LongTimeSeries implements TimeSeries<Long> {
    private static final Set<TimeBucket> VALID_TIMEBUCKETS = new HashSet<>(Arrays.asList(
            TimeBucket.MILLISECOND,
            TimeBucket.SECOND,
            TimeBucket.MINUTE,
            TimeBucket.HOUR,
            TimeBucket.DAY,
            TimeBucket.WEEK,
            TimeBucket.MONTH,
            TimeBucket.YEAR
    ));
    private TimeBucket timeBucket;
    private SortedMap<Long, Long> timeSeries;

    public LongTimeSeries(final TimeBucket timeBucket) {
        if (!VALID_TIMEBUCKETS.contains(timeBucket)) {
            throw new IllegalArgumentException("A TimeBucket of " + timeBucket + " is not supported");
        }
        this.timeBucket = timeBucket;
        this.timeSeries = new TreeMap<>();
    }

    /**
     * Puts the provided <code>value</code> into the time series associated to
     * the {@link Instant} <code>instant</code>. Note that this overwrites any
     * previous value in that bucket.
     *
     * @param instant The instant at which the value was observed.
     * @param value   The value observed at the instant.
     */
    @Override
    public void put(final Instant instant, final Long value) {
        final long bucket = toLong(timeBucket, instant.toEpochMilli());
        timeSeries.put(bucket, value);
    }

    /**
     * Returns the value associated to the given {@link Instant}. Note that this
     * instant is rounded to the nearest time bucket.
     *
     * @param instant The instant that the value is required for.
     * @return The value associated to the instant.
     */
    @Override
    public Long get(final Instant instant) {
        return timeSeries.get(toLong(timeBucket, instant.toEpochMilli()));
    }

    /**
     * Adds the given <code>count</code> to the current value associated to the
     * given {@link Instant}. If there is no value currently associated to the
     * {@link Instant} then the <code>count</code> is simply inserted. Note
     * that the caller of this method is responsible for dealing with the case
     * where adding <code>count</code> would cause an overflow.
     *
     * @param instant The instant at which the value was observed.
     * @param count   The value observed at the instant.
     */
    public void upsert(final Instant instant, final long count) {
        final long bucket = toLong(timeBucket, instant.toEpochMilli());
        timeSeries.merge(bucket, count, (x, y) -> x + y);
    }

    /**
     * Returns a {@link SortedSet} of all the {@link Instant}s in the time series.
     *
     * @return A {@link SortedSet} of all the {@link Instant}s in the time series.
     */
    public SortedSet<Instant> getInstants() {
        final SortedSet<Instant> instants = new TreeSet<>();
        timeSeries
                .keySet()
                .stream()
                .map(l -> getInstantFromLong(timeBucket, l))
                .forEach(instants::add);
        return instants;
    }

    /**
     * Returns the number of instants in the time series.
     *
     * @return The number of instants in the time series.
     */
    public int getNumberOfInstants() {
        return timeSeries.size();
    }

    public SortedMap<Instant, Long> getTimeSeries() {
        final SortedMap<Instant, Long> map = new TreeMap<>();
        timeSeries
                .entrySet()
                .forEach(e -> map.put(getInstantFromLong(timeBucket, e.getKey()), e.getValue()));
        return map;
    }

    public TimeBucket getTimeBucket() {
        return timeBucket;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final LongTimeSeries that = (LongTimeSeries) obj;

        return new EqualsBuilder()
                .append(timeBucket, that.timeBucket)
                .append(timeSeries, that.timeSeries)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(21, 3)
                .append(timeBucket)
                .append(timeSeries)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timeBucket", timeBucket)
                .append("timeSeries", timeSeries)
                .appendSuper(super.toString())
                .build();
    }

    private static long toLong(final TimeBucket timeBucket, final long time) {
        final long timeTruncatedToBucket = CommonTimeUtil.timeToBucket(time, timeBucket);
        switch (timeBucket) {
            case MILLISECOND:
                return timeTruncatedToBucket;
            case SECOND:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_SECOND;
            case MINUTE:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_MINUTE;
            case HOUR:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_HOUR;
            case DAY:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_DAY;
            case WEEK:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_DAY;
            case MONTH:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_DAY;
            case YEAR:
                return timeTruncatedToBucket / CommonTimeUtil.MILLISECONDS_IN_DAY;
            default:
                throw new IllegalStateException("Unknown time bucket of " + timeBucket);
        }
    }

    private static long fromLong(final TimeBucket timeBucket, final long l) {
        switch (timeBucket) {
            case MILLISECOND:
                return l;
            case SECOND:
                return l * CommonTimeUtil.MILLISECONDS_IN_SECOND;
            case MINUTE:
                return l * CommonTimeUtil.MILLISECONDS_IN_MINUTE;
            case HOUR:
                return l * CommonTimeUtil.MILLISECONDS_IN_HOUR;
            case DAY:
                return l * CommonTimeUtil.MILLISECONDS_IN_DAY;
            case WEEK:
                return l * CommonTimeUtil.MILLISECONDS_IN_DAY;
            case MONTH:
                return l * CommonTimeUtil.MILLISECONDS_IN_DAY;
            case YEAR:
                return l * CommonTimeUtil.MILLISECONDS_IN_DAY;
            default:
                throw new IllegalStateException("Unknown time bucket of " + timeBucket);
        }
    }

    private static Instant getInstantFromLong(final TimeBucket timeBucket, final long l) {
        return Instant.ofEpochMilli(fromLong(timeBucket, l));
    }
}
