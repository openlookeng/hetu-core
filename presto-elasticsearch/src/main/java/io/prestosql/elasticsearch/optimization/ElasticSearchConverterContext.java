package io.prestosql.elasticsearch.optimization;

public class ElasticSearchConverterContext {
    private boolean hasConversionFailed;

    public ElasticSearchConverterContext() {
        this.hasConversionFailed = false;
    }

    public boolean isHasConversionFailed() {
        return hasConversionFailed;
    }

    public void setConversionFailed() {
        this.hasConversionFailed = true;
    }
}
