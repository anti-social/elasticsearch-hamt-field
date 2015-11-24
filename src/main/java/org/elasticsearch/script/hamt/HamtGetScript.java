package org.elasticsearch.script.hamt;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import hamt.HAMT;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.hamt.HamtFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;


public class HamtGetScript extends AbstractHamtSearchScript {
    protected final float defaultValue;

    protected HamtGetScript(String fieldName, long key, float defaultValue) {
        super(fieldName, key);
        this.defaultValue = defaultValue;
    }

    @Override
    public float runAsFloat() {
        HamtFieldMapper.HamtFieldType fieldType = (HamtFieldMapper.HamtFieldType) (doc().mapperService().smartNameFieldType(fieldName));
        BinaryDocValues docValues = localDocValuesCache.get(reader);
        if (docValues == null) {
            try {
                docValues = reader.getBinaryDocValues(fieldName);
                localDocValuesCache.put(reader, docValues);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        BytesRef data = docValues.get(docId);
        if (data == null || data.length == 0) {
            return defaultValue;
        }

        HAMT.Reader hamtReader = new HAMT.Reader(data.bytes);
        int valueOffset = hamtReader.getValueOffset(key);
        if (valueOffset == HAMT.Reader.NOT_FOUND_OFFSET) {
            return defaultValue;
        }
        return fieldType.valueType().getValue(hamtReader, valueOffset);
    }

    public static class Factory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
            if (fieldName == null) {
                throw new ScriptException("Missing the field parameter");
            }

            Object keyParam = params.get("key");
            if (keyParam == null) {
                throw new ScriptException("Missing the key parameter");
            }
            long key = params == null ? null : XContentMapValues.nodeLongValue(keyParam);

            float defaultValue = params == null ? null : XContentMapValues.nodeFloatValue(params.get("default"), 0.0f);

            return new HamtGetScript(fieldName, key, defaultValue);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
