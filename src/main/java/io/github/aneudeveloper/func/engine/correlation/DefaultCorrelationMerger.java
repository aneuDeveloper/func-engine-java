package io.github.aneudeveloper.func.engine.correlation;

public class DefaultCorrelationMerger<T> implements CorrelationMerger<T>{

    @Override
    public T mergeCorrelation(T callback, T original, CorrelationContext correlationContext) {
        return original;
    }
    
}
