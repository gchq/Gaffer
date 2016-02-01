package gaffer.rest;

/**
 * System property keys and default values.
 */
public abstract class SystemProperty {
    // KEYS
    public static final String SCHEMA_TYPES_PATH = "gaffer.schemaTypes";
    public static final String DATA_SCHEMA_PATH = "gaffer.dataSchema";
    public static final String STORE_SCHEMA_PATH = "gaffer.storeSchema";
    public static final String STORE_PROPERTIES_PATH = "gaffer.storeProperties";
    public static final String BASE_URL = "gaffer.rest-api.basePath";
    public static final String VERSION = "gaffer.rest-api.version";
    public static final String SERVICES_PACKAGE_PREFIX = "gaffer.rest-api.resourcePackage";
    public static final String PACKAGE_PREFIXES = "gaffer.package.prefixes";

    // DEFAULTS
    /**
     * Comma separated list of package prefixes to search for {@link gaffer.function.Function}s and {@link gaffer.operation.Operation}s.
     */
    public static final String PACKAGE_PREFIXES_DEFAULT = "gaffer";
    public static final String SERVICES_PACKAGE_PREFIX_DEFAULT = "gaffer.rest";
    public static final String BASE_URL_DEFAULT = "gaffer/rest/v1";
    public static final String CORE_VERSION = "1.0.0";
}
