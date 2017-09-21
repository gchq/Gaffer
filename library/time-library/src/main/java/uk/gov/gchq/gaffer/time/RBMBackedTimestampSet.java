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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;

/**
 * An {@code RBMBackedTimestampSet} is an implementation of {@link TimestampSet} that stores timestamps
 * truncated to a certain {@link TimeBucket}, e.g. if a {@link TimeBucket} of a minute is specified then a timestamp
 * of 12:34:56 on January 1st 2015 would be truncated to the previous minute, namely 12:34:00 on January 1st 2015.
 * Timebuckets of second, minute, hour, day, week, month and year are supported.
 * <p>
 * Internally this class stores the timestamps in a {@link RoaringBitmap}.
 * </p>
 * <p>
 * NB: This class does not accept {@link Instant}s that are before the Unix epoch or after the {@link Instant}
 * which is {@code Integer.MAX_VALUE * 1000L} milliseconds after the epoch (approximately 3:14 on January 19th
 * 2038). This is due to {@link RoaringBitmap} only accepting integers. As the smallest {@link TimeBucket} is
 * a second then the maximum supported {@link Instant} is the maximum integer multiplied by 1000L milliseconds after
 * the epoch.
 * </p>
 */
@JsonDeserialize(builder = RBMBackedTimestampSet.Builder.class)
public class RBMBackedTimestampSet implements TimestampSet {
    private static final long MILLISECONDS_IN_SECOND = 1000L;
    private static final long MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final long MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final long MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    private static final Instant MIN_TIME = Instant.ofEpochMilli(0L);
    private static final Instant MAX_TIME = Instant.ofEpochMilli(Integer.MAX_VALUE * MILLISECONDS_IN_SECOND);
    private static final Set<TimeBucket> VALID_TIMEBUCKETS = new HashSet<>(Arrays.asList(
            TimeBucket.SECOND,
            TimeBucket.MINUTE,
            TimeBucket.HOUR,
            TimeBucket.DAY,
            TimeBucket.WEEK,
            TimeBucket.MONTH,
            TimeBucket.YEAR
    ));

    private final TimeBucket timeBucket;
    private RoaringBitmap rbm = new RoaringBitmap();

    public RBMBackedTimestampSet(final TimeBucket timeBucket) {
        if (!VALID_TIMEBUCKETS.contains(timeBucket)) {
            throw new IllegalArgumentException("A TimeBucket of " + timeBucket + " is not supported");
        }
        this.timeBucket = timeBucket;
    }

    public RBMBackedTimestampSet(final TimeBucket timeBucket, final Instant... instants) {
        this(timeBucket);
        Stream.of(instants).forEach(this::add);
    }

    @Override
    public void add(final Instant instant) {
        if (instant.isBefore(MIN_TIME) || instant.isAfter(MAX_TIME)) {
            throw new IllegalArgumentException("Invalid instant of " + instant);
        }
        rbm.add(toInt(instant.toEpochMilli()));
    }

    @Override
    public void add(final Collection<Instant> instants) {
        instants.forEach(this::add);
    }

    @Override
    public SortedSet<Instant> getTimestamps() {
        final SortedSet<Instant> instants = new TreeSet<>();
        rbm.forEach(i -> instants.add(getInstantFromInt(i)));
        return instants;
    }

    @Override
    public long getNumberOfTimestamps() {
        return rbm.getCardinality();
    }

    @Override
    public Instant getEarliest() {
        final IntIterator it = rbm.getIntIterator();
        if (!it.hasNext()) {
            return null;
        }
        return getInstantFromInt(it.next());
    }

    @Override
    public Instant getLatest() {
        final IntIterator it = rbm.getReverseIntIterator();
        if (!it.hasNext()) {
            return null;
        }
        return getInstantFromInt(it.next());
    }

    public TimeBucket getTimeBucket() {
        return timeBucket;
    }

    /**
     * This exposes the underlying {@link RoaringBitmap} so that serialisers can access it.
     *
     * @return the {@link RoaringBitmap} used by this class to store the timestamps.
     */
    @JsonIgnore
    public RoaringBitmap getRbm() {
        return rbm;
    }

    /**
     * Allows the {@link RoaringBitmap} to be set.
     *
     * @param rbm the {@link RoaringBitmap} to set the {@link RoaringBitmap} of this class to.
     */
    public void setRbm(final RoaringBitmap rbm) {
        this.rbm = rbm;
    }

    public void addAll(final RBMBackedTimestampSet other) {
        rbm.or(other.getRbm());
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final RBMBackedTimestampSet rbmBackedTimestampSet = (RBMBackedTimestampSet) obj;

        return new EqualsBuilder()
                .append(timeBucket, rbmBackedTimestampSet.timeBucket)
                .append(rbm, rbmBackedTimestampSet.rbm)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(21, 83)
                .append(timeBucket)
                .append(rbm)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("timeBucket", timeBucket)
                .append("timestamps", StringUtils.join(getTimestamps(), ','))
                .toString();
    }

    private int toInt(final long time) {
        final long timeTruncatedToBucket = CommonTimeUtil.timeToBucket(time, timeBucket);
        switch (timeBucket) {
            case SECOND:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_SECOND);
            case MINUTE:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_MINUTE);
            case HOUR:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_HOUR);
            case DAY:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_DAY);
            case WEEK:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_DAY);
            case MONTH:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_DAY);
            case YEAR:
                return (int) (timeTruncatedToBucket / MILLISECONDS_IN_DAY);
            default:
                throw new IllegalStateException("Unknown time bucket of " + timeBucket);
        }
    }

    private long fromInt(final int i) {
        switch (timeBucket) {
            case SECOND:
                return i * MILLISECONDS_IN_SECOND;
            case MINUTE:
                return i * MILLISECONDS_IN_MINUTE;
            case HOUR:
                return i * MILLISECONDS_IN_HOUR;
            case DAY:
                return i * MILLISECONDS_IN_DAY;
            case WEEK:
                return i * MILLISECONDS_IN_DAY;
            case MONTH:
                return i * MILLISECONDS_IN_DAY;
            case YEAR:
                return i * MILLISECONDS_IN_DAY;
            default:
                throw new IllegalStateException("Unknown time bucket of " + timeBucket);
        }
    }

    private Instant getInstantFromInt(final int i) {
        return Instant.ofEpochMilli(fromInt(i));
    }

    @JsonIgnoreProperties(value = {"numberOfTimestamps", "earliest", "latest"})
    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder {
        private TimeBucket timeBucket;
        private Collection<Instant> timestamps;

        public Builder timeBucket(final TimeBucket timeBucket) {
            this.timeBucket = timeBucket;
            return this;
        }

        public void timestamps(final Collection<Instant> timestamps) {
            this.timestamps = timestamps;
        }

        public RBMBackedTimestampSet build() {
            final RBMBackedTimestampSet set = new RBMBackedTimestampSet(timeBucket);
            if (null != timestamps) {
                set.add(timestamps);
            }
            return set;
        }
    }
}
