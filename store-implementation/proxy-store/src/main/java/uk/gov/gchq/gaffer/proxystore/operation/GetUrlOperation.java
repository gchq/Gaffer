package uk.gov.gchq.gaffer.proxystore.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.HashMap;
import java.util.Map;

@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.17.0")
@Summary("Gets the Proxy URL value from the store properties")
public class GetUrlOperation implements Output<String> {

    private HashMap<String, String> options = new HashMap<>();

    @Override
    public TypeReference<String> getOutputTypeReference() {
        return new TypeReferenceImpl.String();
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new GetUrlOperation.Builder().options(options).build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    public static class Builder extends Operation.BaseBuilder<GetUrlOperation, GetUrlOperation.Builder> {
        public Builder() {
            super(new GetUrlOperation());
        }
    }
}
