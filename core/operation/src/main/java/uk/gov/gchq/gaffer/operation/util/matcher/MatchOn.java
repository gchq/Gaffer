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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatchOn implements Matcher {
    private Field matchingField;

    public MatchOn(final Field matchingField) {
        this.matchingField = matchingField;
    }

    public Field getMatchingField() {
        return matchingField;
    }

    public void setMatchingField(Field matchingField) {
        this.matchingField = matchingField;
    }

    @Override
    public Map matching(final Object testObject, final List listToTest) {
        // Based on the matching field, iterate through the list to test, and
        // if the test object matches the object within the list add it to the results.
        // After this, add the test object and List of results to a Map that will be returned.
        List results = new ArrayList<>();
        Map resultsMap = new HashMap<>();
        for (Object o : listToTest) {
            try {
                if (matchingField.get(testObject).equals(matchingField.get(o))) {
                    results.add(o);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        resultsMap.put(testObject, results);
        return resultsMap;
    }
}
