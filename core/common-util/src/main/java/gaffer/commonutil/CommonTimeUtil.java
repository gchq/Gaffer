/*
 * Copyright 2016 Crown Copyright
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

package gaffer.commonutil;

import org.joda.time.DateTime;
import org.joda.time.DateTime.Property;

import static org.joda.time.DateTimeZone.UTC;

/**
 * Utility methods for dates and times.
 */
public class CommonTimeUtil {

    private CommonTimeUtil() {
        // this class should not be instantiated - it contains only util methods and constants.
    }

    /**
     * Place a time value (a Java {@link Long} representing the number of
     * milliseconds since the start of the Unix epoch) in a {@link TimeBucket}.
     *
     * @param time   the time, in milliseconds since the start of the Unix epoch
     * @param bucket the time bucket to place the time value into
     * @return the value of the time bucket
     */
    public static long timeToBucket(final long time, final TimeBucket bucket) {
        final DateTime dateTime = new DateTime(time, UTC);

        final long timeBucket;

        switch (bucket) {
            case HOUR:
                timeBucket = roundDown(dateTime.hourOfDay());
                break;
            case DAY:
                timeBucket = roundDown(dateTime.dayOfYear());
                break;
            case WEEK:
                timeBucket = roundDown(dateTime.weekOfWeekyear());
                break;
            case MONTH:
                timeBucket = roundDown(dateTime.monthOfYear());
                break;
            default:
                timeBucket = time;
        }

        return timeBucket;
    }

    private static long roundDown(final Property property) {
        return property.roundFloorCopy().getMillis();
    }

    private static long roundUp(final Property property) {
        return property.roundCeilingCopy().getMillis();
    }

    /**
     * Type representing a "bucket" of time.
     */
    public enum TimeBucket {
        HOUR, DAY, WEEK, MONTH, YEAR;
    }
}
