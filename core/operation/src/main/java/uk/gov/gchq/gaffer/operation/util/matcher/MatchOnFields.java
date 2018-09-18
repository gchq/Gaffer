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

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests matches based on a list of fields within a Join Operation.
 */
public class MatchOnFields implements Matcher {
    private List<Field> matchingFields;

    public MatchOnFields(final List<Field> matchingField) {
        this.matchingFields = matchingField;
    }

    public MatchOnFields(final Field... matchingFields) {
        this.matchingFields = Lists.newArrayList(matchingFields);
    }

    public List<Field> getMatchingFields() {
        return matchingFields;
    }

    public void setMatchingFields(final List<Field> matchingFields) {
        this.matchingFields = matchingFields;
    }

    public void addMatchingField(final Field matchingField) {
        if (null != matchingFields) {
            matchingFields.add(matchingField);
        } else {
            matchingFields = Arrays.asList(matchingField);
        }
    }

    @Override
    public List matching(final Object testObject, final List testList) {
        List results = new ArrayList<>();
        for (Object entry : testList) {
            boolean isMatching = true;
            for (Field matchingField : matchingFields) {
                try {
                    if (!matchingField.get(testObject).equals(matchingField.get(entry))) {
                        isMatching = false;
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            if (isMatching) {
                results.add(entry);
            }
        }
        return results;
    }
}
