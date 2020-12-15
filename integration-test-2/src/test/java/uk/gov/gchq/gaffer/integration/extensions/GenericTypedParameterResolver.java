package uk.gov.gchq.gaffer.integration.extensions;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * A Parameter resolver which can resolve any values of the type of object is is constructed with.
 * @param <T> The type of parameter to inject
 */
public class GenericTypedParameterResolver<T> implements ParameterResolver {

    private final T parameter;

    public GenericTypedParameterResolver(T parameter) {
        this.parameter = parameter;
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter()
                .getType()
                .isInstance(parameter);
    }

    @Override
    public T resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameter;
    }
}
