/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;

import java.util.LinkedHashMap;

/**
 * Utility class containing methods for modifying {@link View} objects.
 */
public final class ViewUtil {
    private ViewUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Remove properties from the supplied element, according to the {@link ViewElementDefinition}
     * in the {@link View}.
     *
     * @param view    the view to apply
     * @param element the element to modify
     */
    public static void removeProperties(final View view, final Element element) {
        if (null != view && null != element) {
            removeProperties(view.getElement(element.getGroup()), element);
        }
    }

    /**
     * Remove properties from the supplied element, according to the {@link ViewElementDefinition}.
     *
     * @param elDef   the element definition to apply
     * @param element the element to modify
     */
    public static void removeProperties(final ViewElementDefinition elDef, final Element element) {
        removeProperties(elDef, element.getProperties());
    }

    /**
     * Remove properties from the supplied element, according to the {@link ViewElementDefinition}.
     *
     * @param elDef      the element definition to apply
     * @param properties the properties to modify
     */
    public static void removeProperties(final ViewElementDefinition elDef, final Properties properties) {
        if (null != elDef && !elDef.isAllProperties()) {
            if (null == elDef.getProperties()) {
                elDef.getExcludeProperties().forEach(properties::remove);
            } else {
                properties.keepOnly(elDef.getProperties());
            }
        }
    }

    /**
     * Remove specified from the supplied view.
     * Because The {@link View} is unmodifiable it will return a new {@link View} with the group(s) removed.
     *
     * @param view   the view to apply
     * @param groups the groups to remove
     * @return the new view with the groups removed
     */
    public static View removeGroups(final View view, final String... groups) {
        if (null == view) {
            throw new IllegalArgumentException("View cannot be null");
        }

        final LinkedHashMap<String, ViewElementDefinition> entities = new LinkedHashMap<>(view.getEntities());
        final LinkedHashMap<String, ViewElementDefinition> edges = new LinkedHashMap<>(view.getEdges());

        if (null != groups) {
            for (final String group : groups) {
                entities.remove(group);
                edges.remove(group);
            }
        } else {
            throw new IllegalArgumentException("Specified group(s) to remove is null");
        }

        return new View.Builder()
                .merge(view)
                .entities(entities)
                .edges(edges)
                .build();
    }
}
