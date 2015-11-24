package org.elasticsearch.script.hamt;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;


public abstract class AbstractHamtSearchScript extends AbstractExecutableScript implements LeafSearchScript {
    protected final String fieldName;
    protected final long key;

    protected LeafReader reader;
    protected int docId;

    private LeafSearchLookup lookup;
    private Scorer scorer;

    protected final Map<LeafReader, BinaryDocValues> localDocValuesCache = Maps.newHashMapWithExpectedSize(1);

    protected AbstractHamtSearchScript(String fieldName, long key) {
        this.fieldName = fieldName;
        this.key = key;
    }

    public void setLeafReader(LeafReader reader) {
        this.reader = reader;
    }

    public void setLookup(LeafSearchLookup lookup) {
        this.lookup = lookup;
    }

    protected final LeafDocLookup doc() {
        return lookup.doc();
    }

    @Override
    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
    }

    @Override
    public void setDocument(int doc) {
        docId = doc;
        lookup.setDocument(doc);
    }

    @Override
    public void setSource(Map<String, Object> source) {
        lookup.source().setSource(source);
    }

    @Override
    public Object run() {
        return runAsFloat();
    }

    @Override
    public long runAsLong() {
        return (long) runAsFloat();
    }

    @Override
    public double runAsDouble() {
        return runAsFloat();
    }
}
