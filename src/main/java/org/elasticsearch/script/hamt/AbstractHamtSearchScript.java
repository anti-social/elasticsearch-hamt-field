package org.elasticsearch.script.hamt;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.script.AbstractFloatSearchScript;


public abstract class AbstractHamtSearchScript extends AbstractFloatSearchScript {
    protected final String fieldName;
    protected final long key;

    protected LeafReader reader;
    protected int docId;

    protected final Map<LeafReader, BinaryDocValues> localDocValuesCache = Maps.newHashMapWithExpectedSize(1);

    protected AbstractHamtSearchScript(String fieldName, long key) {
        this.fieldName = fieldName;
        this.key = key;
    }

    public void setLeafReader(LeafReader reader) {
        this.reader = reader;
    }

    @Override
    public void setDocument(int doc) {
        docId = doc;
        super.setDocument(doc);
    }
}
