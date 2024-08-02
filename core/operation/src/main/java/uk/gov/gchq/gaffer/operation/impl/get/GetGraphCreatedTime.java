package uk.gov.gchq.gaffer.operation.impl.get;

import java.util.Map;

import org.apache.commons.lang3.exception.CloneFailedException;

import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;

public class GetGraphCreatedTime implements InputOutput<Iterable<String>, Map<String, String>>
{
    private Map<String, String> options;
    private Iterable<String> graphIds;

    @Override
    public Iterable<String> getInput() {
        return graphIds;
    }

    @Override
    public void setInput(Iterable<String> graphIds) {
        this.graphIds = graphIds;
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .input(graphIds)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public TypeReference<Map<String, String>> getOutputTypeReference() {
        return new TypeReference<Map<String, String>>(){};

    }

    public static class Builder extends Operation.BaseBuilder<GetGraphCreatedTime, Builder> implements InputOutput.Builder<GetGraphCreatedTime, Iterable<String>, Map<String, String>, Builder> {
        public Builder() {
            super(new GetGraphCreatedTime());
        }

    }
    
}
