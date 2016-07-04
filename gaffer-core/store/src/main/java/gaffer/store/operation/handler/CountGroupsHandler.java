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

package gaffer.store.operation.handler;

import gaffer.data.GroupCounts;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.OperationException;
import gaffer.operation.impl.CountGroups;
import gaffer.store.Context;
import gaffer.store.Store;

public class CountGroupsHandler implements OperationHandler<CountGroups, GroupCounts> {
    @Override
    public GroupCounts doOperation(final CountGroups operation,
                                   final Context context, final Store store)
            throws OperationException {
        int count = 0;
        final GroupCounts groupCounts = new GroupCounts();
        if (null != operation.getElements()) {
            for (Element element : operation.getElements()) {
                if (null != operation.getLimit()) {
                    count++;
                    if (count > operation.getLimit()) {
                        groupCounts.setLimitHit(true);
                        break;
                    }
                }

                if (element instanceof Entity) {
                    groupCounts.addEntityGroup(element.getGroup());
                } else {
                    groupCounts.addEdgeGroup(element.getGroup());
                }
            }
        }

        return groupCounts;
    }
}
