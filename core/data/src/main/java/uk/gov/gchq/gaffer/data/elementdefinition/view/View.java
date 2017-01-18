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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * The <code>View</code> defines the {@link uk.gov.gchq.gaffer.data.element.Element}s to be returned for an operation.
 * A view should contain {@link uk.gov.gchq.gaffer.data.element.Edge} and {@link uk.gov.gchq.gaffer.data.element.Entity} types required and
 * for each group it can optionally contain an {@link uk.gov.gchq.gaffer.data.element.function.ElementFilter} and a
 * {@link uk.gov.gchq.gaffer.data.element.function.ElementTransformer}.
 * The {@link uk.gov.gchq.gaffer.function.FilterFunction}s within the ElementFilter describe the how the elements should be filtered.
 * The {@link uk.gov.gchq.gaffer.function.TransformFunction}s within ElementTransformer allow transient properties to be created
 * from other properties and identifiers.
 * It also contains any transient properties that are created in transform functions.
 *
 * @see Builder
 * @see uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition
 * @see uk.gov.gchq.gaffer.data.element.function.ElementFilter
 * @see uk.gov.gchq.gaffer.data.element.function.ElementTransformer
 */
@JsonDeserialize(builder = View.Builder.class)
public class View extends ElementDefinitions<ViewElementDefinition, ViewElementDefinition> implements Cloneable {
    public View() {
        super();
    }

    public byte[] toCompactJson() throws SchemaException {
        return toJson(false);
    }

    @Override
    public String toString() {
        try {
            return "View" + new String(toJson(true), CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ViewElementDefinition getElement(final String group) {
        return (ViewElementDefinition) super.getElement(group);
    }

    public LinkedHashSet<String> getElementGroupBy(final String group) {
        ViewElementDefinition viewElementDef = (ViewElementDefinition) super.getElement(group);
        if (null == viewElementDef) {
            return null;
        }

        return viewElementDef.getGroupBy();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public View clone() {
        return new View.Builder().json(toCompactJson()).build();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends ElementDefinitions.BaseBuilder<View, ViewElementDefinition, ViewElementDefinition, CHILD_CLASS> {
        public BaseBuilder() {
            super(new View());
        }

        @Override
        public CHILD_CLASS entity(final String group) {
            return entity(group, new ViewElementDefinition());
        }

        @Override
        public CHILD_CLASS edge(final String group) {
            return edge(group, new ViewElementDefinition());
        }

        @JsonIgnore
        public CHILD_CLASS json(final InputStream... inputStreams) throws SchemaException {
            return json(View.class, inputStreams);
        }

        @JsonIgnore
        public CHILD_CLASS json(final Path... filePaths) throws SchemaException {
            return json(View.class, filePaths);
        }

        @JsonIgnore
        public CHILD_CLASS json(final byte[]... jsonBytes) throws SchemaException {
            return json(View.class, jsonBytes);
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS merge(final View view) {
            for (final Map.Entry<String, ViewElementDefinition> entry : view.getEntities().entrySet()) {
                if (!getThisView().entities.containsKey(entry.getKey())) {
                    entity(entry.getKey(), entry.getValue());
                } else {
                    final ViewElementDefinition mergedElementDef = new ViewElementDefinition.Builder()
                            .merge(getThisView().entities.get(entry.getKey()))
                            .merge(entry.getValue())
                            .build();
                    getThisView().entities.put(entry.getKey(), mergedElementDef);
                }
            }

            for (final Map.Entry<String, ViewElementDefinition> entry : view.getEdges().entrySet()) {
                if (!getThisView().edges.containsKey(entry.getKey())) {
                    edge(entry.getKey(), entry.getValue());
                } else {
                    final ViewElementDefinition mergedElementDef = new ViewElementDefinition.Builder()
                            .merge(getThisView().edges.get(entry.getKey()))
                            .merge(entry.getValue())
                            .build();
                    getThisView().edges.put(entry.getKey(), mergedElementDef);
                }
            }

            return self();
        }

        private View getThisView() {
            return getElementDefs();
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final View view) {
            this();
            merge(view);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
