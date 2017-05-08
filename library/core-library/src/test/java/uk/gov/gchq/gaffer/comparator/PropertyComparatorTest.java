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
package uk.gov.gchq.gaffer.comparator;

import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;

public abstract class PropertyComparatorTest<T extends Comparable<T>> {

    private final Comparator<T> comparator;
    private final T smallValue;
    private final T bigValue;

    public PropertyComparatorTest() {
        this.comparator = getComparator();
        this.smallValue = getSmallValue();
        this.bigValue = getBigValue();
    }

    @Test
    public void compareForwards() {
        assertThat(bigValue, is(greaterThan(smallValue)));
        assertThat(comparator.compare(bigValue, smallValue), is(greaterThan(0)));
    }

    @Test
    public void compareBackwards() {
        assertThat(smallValue, is(lessThan(bigValue)));
        assertThat(comparator.compare(smallValue, bigValue), is(lessThan(0)));
    }

    @Test
    public void bigValuesShouldBeEqual() {
        assertThat(bigValue, is(equalTo(bigValue)));
        assertThat(comparator.compare(bigValue, bigValue), is(equalTo(0)));
    }

    @Test
    public void smallValuesShouldBeEqual() {
        assertThat(smallValue, is(equalTo(smallValue)));
        assertThat(comparator.compare(smallValue, smallValue), is(equalTo(0)));
    }

    public abstract Comparator<T> getComparator();

    public abstract T getSmallValue();

    public abstract T getBigValue();
}
