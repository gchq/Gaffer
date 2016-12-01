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

package uk.gov.gchq.gaffer.data;

import java.util.HashMap;
import java.util.Map;

/**
 * Summary of element groups. If the limitHit flag is true then the counts will
 * not be fully populated - they are simply the counts of the groups up to the
 * point at which the limit was reached.
 */
public class GroupCounts {
    private Map<String, Integer> entityGroups = new HashMap<>();
    private Map<String, Integer> edgeGroups = new HashMap<>();
    private boolean limitHit;

    public void addEntityGroup(final String group) {
        addElementGroup(group, entityGroups);
    }

    public void addEdgeGroup(final String group) {
        addElementGroup(group, edgeGroups);
    }

    private void addElementGroup(final String group, final Map<String, Integer> elementGroups) {
        Integer count = elementGroups.get(group);
        if (null == count) {
            count = 1;
        } else {
            count += 1;
        }

        elementGroups.put(group, count);
    }

    public boolean isLimitHit() {
        return limitHit;
    }

    public void setLimitHit(final boolean limitHit) {
        this.limitHit = limitHit;
    }

    public Map<String, Integer> getEntityGroups() {
        return entityGroups;
    }

    public void setEntityGroups(final Map<String, Integer> entityGroups) {
        this.entityGroups = entityGroups;
    }

    public Map<String, Integer> getEdgeGroups() {
        return edgeGroups;
    }

    public void setEdgeGroups(final Map<String, Integer> edgeGroups) {
        this.edgeGroups = edgeGroups;
    }

    @Override
    public String toString() {
        return "GroupCounts{"
                + "entityGroups=" + entityGroups
                + ", edgeGroups=" + edgeGroups
                + ", limitHit=" + limitHit
                + '}';
    }
}
