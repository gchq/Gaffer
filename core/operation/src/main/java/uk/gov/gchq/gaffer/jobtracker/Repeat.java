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

package uk.gov.gchq.gaffer.jobtracker;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Repeat implements Serializable {

    private static final long serialVersionUID = 8514342090490992995L;
    private long initialDelay;
    private long repeatPeriod;
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    public Repeat() {
    }

    public Repeat(final long initialDelay, final long repeatPeriod, final TimeUnit timeUnit) {
        this.initialDelay = initialDelay;
        this.repeatPeriod = repeatPeriod;
        this.timeUnit = timeUnit;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(final long initialDelay) {
        this.initialDelay = initialDelay;
    }

    public long getRepeatPeriod() {
        return repeatPeriod;
    }

    public void setRepeatPeriod(final long repeatPeriod) {
        this.repeatPeriod = repeatPeriod;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(final TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }
        final Repeat repeat = (Repeat) obj;
        return new EqualsBuilder()
                .append(initialDelay, repeat.initialDelay)
                .append(repeatPeriod, repeat.repeatPeriod)
                .append(timeUnit, repeat.timeUnit)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 53)
                .append(initialDelay)
                .append(repeatPeriod)
                .append(timeUnit)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("initialDelay", initialDelay)
                .append("repeatPeriod", repeatPeriod)
                .append("timeUnit", timeUnit)
                .toString();
    }
}
