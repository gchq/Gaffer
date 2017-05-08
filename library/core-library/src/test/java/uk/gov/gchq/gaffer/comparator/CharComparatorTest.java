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

import java.util.Comparator;

public class CharComparatorTest extends PropertyComparatorTest<Character> {

    @Override
    public Comparator<Character> getComparator() {
        return new CharComparator();
    }

    @Override
    public Character getSmallValue() {
        return Character.MIN_VALUE;
    }

    @Override
    public Character getBigValue() {
        return Character.MAX_VALUE;
    }
}
