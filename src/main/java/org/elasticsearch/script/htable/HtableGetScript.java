package org.elasticsearch.script.htable;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import net.uaprom.htable.HashTable;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.htable.HtableFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;


public class HtableGetScript extends AbstractHtableSearchScript {
    protected final float defaultValue;

    protected HtableGetScript(String fieldName, long key, float defaultValue) {
        super(fieldName, key);
        this.defaultValue = defaultValue;
    }

    @Override
    public float runAsFloat() {
        HtableFieldMapper.HtableFieldType fieldType = (HtableFieldMapper.HtableFieldType) (doc().mapperService().smartNameFieldType(fieldName));
        if (fieldType == null) {
            throw new IllegalStateException("No field found for [" + fieldName + "]; expected [htable] field type");
        }
        
        BinaryDocValues docValues = localDocValuesCache.get(reader);
        if (docValues == null) {
            try {
                docValues = reader.getBinaryDocValues(fieldName);
                localDocValuesCache.put(reader, docValues);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        if (docValues == null) {
            return defaultValue;
        }

        BytesRef data = docValues.get(docId);
        if (data == null || data.length == 0) {
            return defaultValue;
        }

        HashTable.Reader htableReader = fieldType.hashTableReader(data);
        int valueOffset = htableReader.getValueOffset(key);
        if (valueOffset == HashTable.Reader.NOT_FOUND_OFFSET) {
            return defaultValue;
        }
        return fieldType.valueType().getValue(htableReader, valueOffset);
    }

    public static class Factory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
            if (fieldName == null) {
                throw new ScriptException("Missing the [field] parameter");
            }

            Object keyParam = params.get("key");
            if (keyParam == null) {
                throw new ScriptException("Missing the [key] parameter");
            }
            long key = params == null ? null : XContentMapValues.nodeLongValue(keyParam);

            float defaultValue = params == null ? null : XContentMapValues.nodeFloatValue(params.get("default"), 0.0f);

            return new HtableGetScript(fieldName, key, defaultValue);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
