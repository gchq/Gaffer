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

package uk.gov.gchq.gaffer.operation.util.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MergeOn implements Merge {
    private List<String> leftFields = new ArrayList<>();
    private List<String> rightFields = new ArrayList<>();

    @Override
    public Iterable reduce(final Iterable results) {
        List<Object> reducedResults = new ArrayList<>();
        //Set(Map(Object,List(relatedObjects));
        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) results) {
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                // Each key within the map entry
                // Need to get relevant bits out of the Object
                mapEntry.getKey();
                    for(Object obj : mapEntry.getValue()){
                        //Each Object within the related object
                    }
            }
        }
        return results;
    }

    public List<String> getLeftFields() {
        return leftFields;
    }

    public void setLeftFields(final List<String> leftFields) {
        this.leftFields = leftFields;
    }

    public List<String> getRightFields() {
        return rightFields;
    }

    public void setRightFields(final List<String> rightFields) {
        this.rightFields = rightFields;
    }
}
