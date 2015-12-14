/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.predicate.time.impl;

import gaffer.predicate.time.TimePredicate;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * A predicate to indicate whether the start and end dates are within the provided {@link Date}s.
 */
public class TimeWindowPredicate extends TimePredicate {

    private static final long serialVersionUID = 3752245040055996456L;
    private Date startTimeWindow;
    private Date endTimeWindow;

    public TimeWindowPredicate() { }

    public TimeWindowPredicate(Date startTimeWindow, Date endTimeWindow) {
        if (startTimeWindow.after(endTimeWindow)) {
            throw new IllegalArgumentException("Start time window must be before end time window");
        }
        this.startTimeWindow = startTimeWindow;
        this.endTimeWindow = endTimeWindow;
    }

    @Override
    public boolean accept(Date startDate, Date endDate) {
        return startTimeWindow.compareTo(startDate) <= 0 && endDate.compareTo(endTimeWindow) <= 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, startTimeWindow.getTime());
        WritableUtils.writeVLong(out, endTimeWindow.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startTimeWindow = new Date(WritableUtils.readVLong(in));
        endTimeWindow = new Date(WritableUtils.readVLong(in));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeWindowPredicate that = (TimeWindowPredicate) o;

        if (endTimeWindow != null ? !endTimeWindow.equals(that.endTimeWindow) : that.endTimeWindow != null)
            return false;
        if (startTimeWindow != null ? !startTimeWindow.equals(that.startTimeWindow) : that.startTimeWindow != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = startTimeWindow != null ? startTimeWindow.hashCode() : 0;
        result = 31 * result + (endTimeWindow != null ? endTimeWindow.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TimeWindowPredicate{" +
                "startTimeWindow=" + startTimeWindow +
                ", endTimeWindow=" + endTimeWindow +
                '}';
    }
}
