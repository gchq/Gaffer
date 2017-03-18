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
package uk.gov.gchq.koryphe.predicate;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class KoryphePredicate<T> implements IKoryphePredicate<T> {
    @SuppressFBWarnings(value = "BC_EQUALS_METHOD_SHOULD_WORK_FOR_ALL_OBJECTS", justification = "the method classEquals does the check")
    @Override
    public boolean equals(final Object other) {
        return this == other || (null != other && getClass().equals(other.getClass()));
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
