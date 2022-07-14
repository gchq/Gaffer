/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.federatedstore.FederatedGraphStorage;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * <p>
 * An Operation used for adding graphs to a FederatedStore.
 * </p>
 * Requires:
 * <ul>
 * <li>graphId
 * <li>storeProperties and/or parentPropertiesId</li>
 * <li>schema and/or parentSchemaIds</li>
 * </ul>
 * <p>
 * parentId can be used solely, if known by the graphLibrary.
 * </p>
 * <p>
 * schema can be used solely.
 * </p>
 * <p>
 * storeProperties can be used, if authorised to by {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore#isLimitedToLibraryProperties(uk.gov.gchq.gaffer.user.User)}
 * both non-parentId and parentId can be used, and will be merged together.
 * </p>
 *
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.store.schema.Schema
 * @see uk.gov.gchq.gaffer.data.element.Properties
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
@JsonPropertyOrder(value = {"class", "graphId"}, alphabetic = true)
@Since("1.0.0")
@Summary("Adds a new Graph to the federated store")
@JsonInclude(Include.NON_DEFAULT)
public class AddGraph implements IFederationOperation {
    @Required
    private String graphId;
    private StoreProperties storeProperties;
    private String parentPropertiesId;
    private Schema schema;
    private List<String> parentSchemaIds;
    private Set<String> graphAuths;
    private Map<String, String> options;
    private boolean isPublic = false;
    private boolean disabledByDefault = FederatedGraphStorage.DEFAULT_DISABLED_BY_DEFAULT;
    private AccessPredicate readAccessPredicate;
    private AccessPredicate writeAccessPredicate;
    private boolean userRequestingAdminUsage;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public AddGraph shallowClone() throws CloneFailedException {
        final Builder builder = new Builder()
                .graphId(this.graphId)
                .schema(this.schema)
                .storeProperties(this.storeProperties)
                .parentSchemaIds(this.parentSchemaIds)
                .parentPropertiesId(this.parentPropertiesId)
                .disabledByDefault(this.disabledByDefault)
                .options(this.options)
                .isPublic(this.isPublic)
                .readAccessPredicate(this.readAccessPredicate)
                .writeAccessPredicate(this.writeAccessPredicate)
                .userRequestingAdminUsage(this.userRequestingAdminUsage);

        if (null != graphAuths) {
            builder.graphAuths(graphAuths.toArray(new String[graphAuths.size()]));
        }

        return builder.build();
    }

    public List<String> getParentSchemaIds() {
        return parentSchemaIds;
    }

    public void setParentSchemaIds(final List<String> parentSchemaIds) {
        this.parentSchemaIds = parentSchemaIds;
    }

    @JsonIgnore
    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    @JsonIgnore
    public void setStoreProperties(final StoreProperties properties) {
        this.storeProperties = properties;
    }

    public String getParentPropertiesId() {
        return parentPropertiesId;
    }

    public void setParentPropertiesId(final String parentPropertiesId) {
        this.parentPropertiesId = parentPropertiesId;
    }

    public boolean isDisabledByDefault() {
        return disabledByDefault;
    }

    public void setDisabledByDefault(final boolean disabledByDefault) {
        this.disabledByDefault = disabledByDefault;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Set<String> getGraphAuths() {
        return graphAuths;
    }

    public void setGraphAuths(final Set<String> graphAuths) {
        this.graphAuths = graphAuths;
    }

    @JsonGetter("storeProperties")
    public Properties getProperties() {
        return null != storeProperties ? storeProperties.getProperties() : null;
    }

    @JsonSetter("storeProperties")
    public void setProperties(final Properties properties) {
        if (null == properties) {
            setStoreProperties(null);
        } else {
            setStoreProperties(StoreProperties.loadStoreProperties(properties));
        }
    }

    public void setIsPublic(final boolean isPublic) {
        this.isPublic = isPublic;
    }

    public boolean getIsPublic() {
        return isPublic;
    }

    public AccessPredicate getWriteAccessPredicate() {
        return writeAccessPredicate;
    }

    public void setWriteAccessPredicate(final AccessPredicate writeAccessPredicate) {
        this.writeAccessPredicate = writeAccessPredicate;
    }

    public AccessPredicate getReadAccessPredicate() {
        return readAccessPredicate;
    }

    public void setReadAccessPredicate(final AccessPredicate readAccessPredicate) {
        this.readAccessPredicate = readAccessPredicate;
    }

    @Override
    public boolean isUserRequestingAdminUsage() {
        return userRequestingAdminUsage;
    }

    @Override
    public AddGraph isUserRequestingAdminUsage(final boolean adminRequest) {
        userRequestingAdminUsage = adminRequest;
        return this;
    }

    public abstract static class GraphBuilder<OP extends AddGraph, B extends GraphBuilder<OP, ?>> extends BaseBuilder<OP, B> {

        protected GraphBuilder(final OP addGraph) {
            super(addGraph);
        }

        public B graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public B storeProperties(final StoreProperties storeProperties) {
            _getOp().setStoreProperties(storeProperties);
            return _self();
        }

        public B schema(final Schema schema) {
            _getOp().setSchema(schema);
            return _self();
        }

        public B parentPropertiesId(final String parentPropertiesId) {
            this._getOp().setParentPropertiesId(parentPropertiesId);
            return _self();
        }

        public B parentSchemaIds(final List<String> parentSchemaIds) {
            _getOp().setParentSchemaIds(parentSchemaIds);
            return _self();
        }

        public B isPublic(final boolean isPublic) {
            _getOp().setIsPublic(isPublic);
            return _self();
        }

        public B graphAuths(final String... graphAuths) {
            if (null == graphAuths) {
                _getOp().setGraphAuths(null);
            } else {
                _getOp().setGraphAuths(Sets.newHashSet(graphAuths));
            }
            return _self();
        }

        public B disabledByDefault(final boolean disabledByDefault) {
            _getOp().setDisabledByDefault(disabledByDefault);
            return _self();
        }

        public B readAccessPredicate(final AccessPredicate readAccessPredicate) {
            _getOp().setReadAccessPredicate(readAccessPredicate);
            return _self();
        }

        public B writeAccessPredicate(final AccessPredicate writeAccessPredicate) {
            _getOp().setWriteAccessPredicate(writeAccessPredicate);
            return _self();
        }
    }

    public static class Builder extends GraphBuilder<AddGraph, Builder> {
        public Builder() {
            super(new AddGraph());
        }
    }
}
