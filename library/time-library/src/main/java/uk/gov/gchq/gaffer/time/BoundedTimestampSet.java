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

import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.OptionalLong;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A <code>BoundedTimestampSet</code> is an implementation of {@link TimestampSet} that can contain a maximum number
 * N of timestamps. If more than N timestamps are added then a uniform random sample of size approximately N of the
 * timestamps is retained.
 *
 * <p>This is useful in avoiding the in-memory or serialised size of the set of timestamps becoming too
 * large. If less than N timestamps are added then the timestamps are stored in a {@link RBMBackedTimestampSet}. If
 * more than N timestamps are added then a uniform random sample of size N of the timestamps is retained.
 */
public class BoundedTimestampSet implements TimestampSet {
    public enum State {
        NOT_FULL,
        SAMPLE
    }

    private final TimeBucket bucket;
    private final int maxSize;
    private State state;
    private RBMBackedTimestampSet rbmBackedTimestampSet;
    private ReservoirLongsUnion reservoirLongsUnion;

    public BoundedTimestampSet(final TimeBucket bucket, final int maxSize) {
        this.bucket = bucket;
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Maximum size must be strictly positive");
        }
        this.maxSize = maxSize;
        this.state = State.NOT_FULL;
        this.rbmBackedTimestampSet = new RBMBackedTimestampSet(bucket);
    }

    @Override
    public void add(final Instant instant) {
        if (state.equals(State.NOT_FULL)) {
            rbmBackedTimestampSet.add(instant);
            checkSize();
        } else {
            reservoirLongsUnion.update(CommonTimeUtil.timeToBucket(instant.toEpochMilli(), bucket));
        }
    }

    @Override
    public void add(final Collection<Instant> instants) {
        instants.forEach(this::add);
    }

    @Override
    public SortedSet<Instant> get() {
        if (state.equals(State.NOT_FULL)) {
            return rbmBackedTimestampSet.get();
        }
        final SortedSet<Instant> instants = new TreeSet<>();
        for (final long l : reservoirLongsUnion.getResult().getSamples()) {
            instants.add(Instant.ofEpochMilli(l));
        }
        return instants;
    }

    @Override
    public long getNumberOfTimestamps() {
        if (state.equals(State.NOT_FULL)) {
            return rbmBackedTimestampSet.getNumberOfTimestamps();
        }
        return (long) reservoirLongsUnion.getResult().getNumSamples();
    }

    @Override
    public Instant getEarliest() {
        if (state.equals(State.NOT_FULL)) {
            return rbmBackedTimestampSet.getEarliest();
        }
        final OptionalLong earliestLong = Arrays.stream(reservoirLongsUnion.getResult().getSamples()).min();
        if (!earliestLong.isPresent()) {
            throw new IllegalStateException("BoundedTimestampSet was in sample mode, but no values were present");
        }
        return Instant.ofEpochMilli(earliestLong.getAsLong());
    }

    @Override
    public Instant getLatest() {
        if (state.equals(State.NOT_FULL)) {
            return rbmBackedTimestampSet.getLatest();
        }
        final OptionalLong latestLong = Arrays.stream(reservoirLongsUnion.getResult().getSamples()).max();
        if (!latestLong.isPresent()) {
            throw new IllegalStateException("BoundedTimestampSet was in sample mode, but no values were present");
        }
        return Instant.ofEpochMilli(latestLong.getAsLong());
    }

    public TimeBucket getTimeBucket() {
        return bucket;
    }

    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Returns {@link State#NOT_FULL} if less than or equal to {@link #getMaxSize()} timestamps have been added.
     * Otherwise {@link State#SAMPLE} is returned.
     *
     * @return The {@link State} that this object is currently in.
     */
    public State getState() {
        return state;
    }

    /**
     * This exposes the underlying {@link RBMBackedTimestampSet} so that serialisers can access it. If
     * the object is currently in the state of <code>NOT_FULL</code> then an exception is thrown.
     *
     * @return the {@link RBMBackedTimestampSet} used by this class to store the timestamps if it is in state
     * <code>NOT_FULL</code>
     */
    public RBMBackedTimestampSet getRBMBackedTimestampSet() {
        if (!state.equals(State.NOT_FULL)) {
            throw new RuntimeException("Cannot access the RoaringBitmap if the state of the object is SAMPLE");
        }
        return rbmBackedTimestampSet;
    }

    /**
     * Allows the {@link RBMBackedTimestampSet} to be set.
     *
     * @param rbmBackedTimestampSet the {@link RBMBackedTimestampSet} to set the {@link RBMBackedTimestampSet} of this class to.
     */
    public void setRBMBackedTimestampSet(final RBMBackedTimestampSet rbmBackedTimestampSet) {
        state = State.NOT_FULL;
        this.rbmBackedTimestampSet = rbmBackedTimestampSet;
    }

    /**
     * This exposes the underlying {@link ReservoirLongsUnion} so that serialisers can access it. If the object is currently
     * in the state of <code>NOT_FULL</code> then an exception is thrown.
     *
     * @return the {@link ReservoirLongsUnion} used by this class to store the timestamps if it is in state <code>SAMPLE</code>
     */
    public ReservoirLongsUnion getReservoirLongsUnion() {
        if (!state.equals(State.SAMPLE)) {
            throw new RuntimeException("Cannot access the ReservoirLongsUnion if the state of the object is NOT_FULL");
        }
        return reservoirLongsUnion;
    }

    /**
     * Allows the {@link ReservoirLongsUnion} to be set.
     *
     * @param reservoirLongsUnion the {@link ReservoirLongsUnion} to be set.
     */
    public void setReservoirLongsUnion(final ReservoirLongsUnion reservoirLongsUnion) {
        state = State.SAMPLE;
        this.reservoirLongsUnion = reservoirLongsUnion;
    }

    private void checkSize() {
        if (null != rbmBackedTimestampSet && rbmBackedTimestampSet.getNumberOfTimestamps() > maxSize) {
            switchToSampleState();
        }
    }

    public void switchToSampleState() {
        if (getState().equals(State.SAMPLE)) {
            return;
        }
        // Switch state from RBM to ReservoirLongsUnion, copy values from RBM to the reservoir, and set the
        // RBM to null.
        state = State.SAMPLE;
        reservoirLongsUnion = ReservoirLongsUnion.getInstance(maxSize);
        for (final Instant instant : rbmBackedTimestampSet.get()) {
            reservoirLongsUnion.update(instant.toEpochMilli());
        }
        rbmBackedTimestampSet = null;
    }
}
