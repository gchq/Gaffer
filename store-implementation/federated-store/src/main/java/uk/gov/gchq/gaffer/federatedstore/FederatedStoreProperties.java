/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.InputStream;
import java.nio.file.Path;


/**
 * Additional {@link StoreProperties} for the {@link FederatedStore}.
 */
public class FederatedStoreProperties extends StoreProperties {
    /**
     * Controls if adding graphs with public access is allowed.
     * True by default.
     * e.g. gaffer.federatedstore.isPublicAllowed=true
     */
    public static final String IS_PUBLIC_ACCESS_ALLOWED = "gaffer.federatedstore.isPublicAllowed";
    public static final String IS_PUBLIC_ACCESS_ALLOWED_DEFAULT = String.valueOf(true);
    /**
     * String containing auths for allowing users to use store properties other than those contained in a graph library.
     * Unset by default, allowing all users to do this.
     * e.g. gaffer.federatedstore.customPropertiesAuths="auth1"
     */
    public static final String CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";
    public static final String CUSTOM_PROPERTIES_AUTHS_DEFAULT = null;
    public static final String CACHE_SERVICE_CLASS_DEFAULT = HashMapCacheService.class.getCanonicalName();
    public static final String STORE_CONFIGURED_MERGE_FUNCTIONS = "gaffer.federatedstore.storeConfiguredMergeFunctions";
    public static final String STORE_CONFIGURED_GRAPHIDS = "gaffer.federatedstore.storeConfiguredGraphIds";
    public static final String CACHE_SERVICE_FEDERATED_STORE_SUFFIX = "gaffer.cache.service.federated.store.suffix";
    /**
     * Name of the system property to use for defining a cache service class dedicated to the Federated Store.
     */
    public static final String CACHE_SERVICE_FEDERATED_STORE_CLASS = "gaffer.cache.service.federatedstore.class";

    public FederatedStoreProperties() {
        super(FederatedStore.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final String pathStr) {
        return StoreProperties.loadStoreProperties(pathStr, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return StoreProperties.loadStoreProperties(storePropertiesStream, FederatedStoreProperties.class);
    }

    public static FederatedStoreProperties loadStoreProperties(final Path storePropertiesPath) {
        return StoreProperties.loadStoreProperties(storePropertiesPath, FederatedStoreProperties.class);
    }

    public void setCustomPropertyAuths(final String auths) {
        set(CUSTOM_PROPERTIES_AUTHS, auths);
    }

    public String getCustomPropsValue() {
        return this.get(CUSTOM_PROPERTIES_AUTHS, CUSTOM_PROPERTIES_AUTHS_DEFAULT);
    }

    public String getIsPublicAccessAllowed() {
        return getIsPublicAccessAllowed(IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    }

    public String getIsPublicAccessAllowed(final String defaultValue) {
        return get(IS_PUBLIC_ACCESS_ALLOWED, defaultValue);
    }

    public void setFalseGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(false);
    }

    public void setTrueGraphsCanHavePublicAccess() {
        setGraphsCanHavePublicAccess(true);
    }

    public void setGraphsCanHavePublicAccess(final boolean b) {
        set(IS_PUBLIC_ACCESS_ALLOWED, Boolean.toString(b));
    }

    public String getStoreConfiguredMergeFunctions() {
        return get(STORE_CONFIGURED_MERGE_FUNCTIONS);
    }

    public void setStoreConfiguredMergeFunctions(final String mergeFunctionFile) {
        set(STORE_CONFIGURED_MERGE_FUNCTIONS, mergeFunctionFile);
    }

    public String getStoreConfiguredGraphIds() {
        return get(STORE_CONFIGURED_GRAPHIDS);
    }

    public void setStoreConfiguredGraphIds(final String mergeFunctionFile) {
        set(STORE_CONFIGURED_GRAPHIDS, mergeFunctionFile);
    }

    public String getCacheServiceFederatedStoreSuffix(final String defaultValue) {
        return getCacheServiceFederatedStoreSuffix(this, defaultValue);
    }

    public static String getCacheServiceFederatedStoreSuffix(final StoreProperties properties, final String defaultValue) {
        return properties.get(CACHE_SERVICE_FEDERATED_STORE_SUFFIX, properties.getCacheServiceDefaultSuffix(defaultValue));
    }

    public String getFederatedStoreCacheServiceClass() {
        return get(CACHE_SERVICE_FEDERATED_STORE_CLASS);
    }

    public void setFederatedStoreCacheServiceClass(final String cacheServiceClassString) {
        set(CACHE_SERVICE_FEDERATED_STORE_CLASS, cacheServiceClassString);
    }
}
