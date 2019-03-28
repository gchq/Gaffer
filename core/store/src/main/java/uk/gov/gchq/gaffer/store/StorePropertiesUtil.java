package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.util.HashSet;
import java.util.Set;

public class StorePropertiesUtil {
    public static final String STORE_CLASS = "gaffer.store.class";
    public static final String SCHEMA_CLASS = "gaffer.store.schema.class";
    /**
     * @deprecated the ID should not be used. The properties ID should be supplied to the graph library separately.
     */
    @Deprecated
    public static final String ID = "gaffer.store.id";

    public static final String OPERATION_DECLARATIONS = "gaffer.store.operation.declarations";

    public static final String JOB_TRACKER_ENABLED = "gaffer.store.job.tracker.enabled";

    public static final String EXECUTOR_SERVICE_THREAD_COUNT = "gaffer.store.job.executor.threads";
    public static final String EXECUTOR_SERVICE_THREAD_COUNT_DEFAULT = "50";

    public static final String JSON_SERIALISER_CLASS = JSONSerialiser.JSON_SERIALISER_CLASS_KEY;
    public static final String JSON_SERIALISER_MODULES = JSONSerialiser.JSON_SERIALISER_MODULES;
    public static final String STRICT_JSON = JSONSerialiser.STRICT_JSON;

    public static final String ADMIN_AUTH = "gaffer.store.admin.auth";

    /**
     * Comma Separated List of extra packages to be included in the reflection scanning.
     */
    public static final String REFLECTION_PACKAGES = "gaffer.store.reflection.packages";


    /**
     * Returns the operation definitions from the file specified in the
     * properties.
     * This is an optional feature, so if the property does not exist then this
     * function
     * will return an empty object.
     *
     * @param properties to be retrieved from
     * @return The Operation Definitions to load dynamically
     */
    @JsonIgnore
    public static OperationDeclarations getOperationDeclarations(final StoreProperties properties) {
        OperationDeclarations declarations = null;

        final String declarationsPaths = properties.getProperty(OPERATION_DECLARATIONS);
        if (null != declarationsPaths) {
            declarations = OperationDeclarations.fromPaths(declarationsPaths);
        }

        if (null == declarations) {
            declarations = new OperationDeclarations.Builder().build();
        }

        return declarations;
    }

    public static Boolean getJobTrackerEnabled(final StoreProperties properties) {
        return Boolean.valueOf(properties.getProperty(JOB_TRACKER_ENABLED, "false"));
    }

    public static void setJobTrackerEnabled(final StoreProperties properties, final Boolean jobTrackerEnabled) {
        properties.setProperty(JOB_TRACKER_ENABLED, jobTrackerEnabled.toString());
    }

    public static String getStoreClass(final StoreProperties storeProperties) {
        return storeProperties.getProperty(STORE_CLASS);
    }

    public static void setStoreClass(final StoreProperties storeProperties, final Class<? extends Store> storeClass) {
        setStoreClass(storeProperties, storeClass.getName());
    }

    public static void setStoreClass(final StoreProperties storeProperties, final String storeClass) {
        storeProperties.setProperty(STORE_CLASS, storeClass);
    }

    public static String getSchemaClassName(final StoreProperties properties) {
        return properties.getProperty(SCHEMA_CLASS, Schema.class.getName());
    }

    public static Class<? extends Schema> getSchemaClass(final StoreProperties properties) {
        final Class<? extends Schema> schemaClass;
        try {
            schemaClass = Class.forName(getSchemaClassName(properties)).asSubclass(Schema.class);
        } catch (final ClassNotFoundException e) {
            throw new SchemaException("Schema class was not found: " + getSchemaClassName(properties), e);
        }

        return schemaClass;
    }

    @JsonSetter
    public static void setSchemaClass(final StoreProperties properties, final String schemaClass) {
        properties.setProperty(SCHEMA_CLASS, schemaClass);
    }

    public static void setSchemaClass(final StoreProperties properties, final Class<? extends Schema> schemaClass) {
        properties.setProperty(SCHEMA_CLASS, schemaClass.getName());
    }

    public static String getOperationDeclarationPaths(final StoreProperties properties) {
        return properties.getProperty(OPERATION_DECLARATIONS);
    }

    public static void setOperationDeclarationPaths(final StoreProperties properties, final String paths) {
        properties.setProperty(OPERATION_DECLARATIONS, paths);
    }

    public static String getReflectionPackages(final StoreProperties properties) {
        return properties.getProperty(REFLECTION_PACKAGES);
    }

    public static void setReflectionPackages(final StoreProperties properties, final String packages) {
        properties.setProperty(REFLECTION_PACKAGES, packages);
        ReflectionUtil.addReflectionPackages(packages);
    }

    public static Integer getJobExecutorThreadCount(final StoreProperties properties) {
        return Integer.parseInt(properties.getProperty(EXECUTOR_SERVICE_THREAD_COUNT, EXECUTOR_SERVICE_THREAD_COUNT_DEFAULT));
    }

    public static void addOperationDeclarationPaths(final StoreProperties properties, final String... newPaths) {
        final String newPathsCsv = StringUtils.join(newPaths, ",");
        String combinedPaths = getOperationDeclarationPaths(properties);
        if (null == combinedPaths) {
            combinedPaths = newPathsCsv;
        } else {
            combinedPaths = combinedPaths + "," + newPathsCsv;
        }
        setOperationDeclarationPaths(properties, combinedPaths);
    }

    public static String getJsonSerialiserClass(final StoreProperties properties) {
        return properties.getProperty(JSON_SERIALISER_CLASS);
    }

    @JsonIgnore
    public static void setJsonSerialiserClass(final StoreProperties properties, final Class<? extends JSONSerialiser> jsonSerialiserClass) {
        setJsonSerialiserClass(properties, jsonSerialiserClass.getName());
    }

    public static void setJsonSerialiserClass(final StoreProperties properties, final String jsonSerialiserClass) {
        properties.setProperty(JSON_SERIALISER_CLASS, jsonSerialiserClass);
    }

    public static String getJsonSerialiserModules(final StoreProperties properties) {
        return properties.getProperty(JSON_SERIALISER_MODULES, "");
    }

    @JsonIgnore
    public static void setJsonSerialiserModules(final StoreProperties properties, final Set<Class<? extends JSONSerialiserModules>> modules) {
        final Set<String> moduleNames = new HashSet<>(modules.size());
        for (final Class module : modules) {
            moduleNames.add(module.getName());
        }
        setJsonSerialiserModules(properties, StringUtils.join(moduleNames, ","));
    }

    public static void setJsonSerialiserModules(final StoreProperties properties, final String modules) {
        properties.setProperty(JSON_SERIALISER_MODULES, modules);
    }

    public static Boolean getStrictJson(final StoreProperties properties) {
        final String strictJson = properties.getProperty(STRICT_JSON);
        return null == strictJson ? null : Boolean.parseBoolean(strictJson);
    }

    public static void setStrictJson(final StoreProperties properties, final Boolean strictJson) {
        properties.setProperty(STRICT_JSON, null == strictJson ? null : Boolean.toString(strictJson));
    }

    public static String getAdminAuth(final StoreProperties properties) {
        return properties.getProperty(ADMIN_AUTH, "");
    }

    public static void setAdminAuth(final StoreProperties properties, final String adminAuth) {
        properties.setProperty(ADMIN_AUTH, adminAuth);
    }
}
