/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.util.matcher;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests matches based on a field within a Join Operation.
 */
public class MatchOnField implements Matcher {
    private Field matchingField;

    public MatchOnField(final Field matchingField) {
        this.matchingField = matchingField;
    }

    public Field getMatchingField() {
        return matchingField;
    }

    public void setMatchingField(Field matchingField) {
        this.matchingField = matchingField;
    }

    /**
     * Returns a list of matches based on a field to match on.
     *
     * @param testObject Object to test against.
     * @param testList   List to test against.
     * @return List containing matched Objects.
     */
    @Override
    public List matching(final Object testObject, final List testList) {
        List results = new ArrayList<>();
        for (Object entry : testList) {
            try {
                if (matchingField.get(testObject).equals(matchingField.get(entry))) {
                    results.add(entry);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return results;
    }
}
