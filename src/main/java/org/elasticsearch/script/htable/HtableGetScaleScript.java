package org.elasticsearch.script.htable;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import net.uaprom.htable.HashTable;
import net.uaprom.htable.TrieHashTable;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.htable.HtableFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;


public class HtableGetScaleScript extends AbstractHtableSearchScript {
    private final float defaultValue;

    private final float[] scaleTable;

    private HtableGetScaleScript(String fieldName, long key, float defaultValue, float[] scaleTable) {
        super(fieldName, key);
        this.defaultValue = defaultValue;
        this.scaleTable = scaleTable;
    }

    @Override
    public float runAsFloat() {
        HtableFieldMapper.HtableFieldType fieldType = (HtableFieldMapper.HtableFieldType) (doc().mapperService().smartNameFieldType(fieldName));
        if (fieldType == null) {
            throw new IllegalStateException("No field found for [" + fieldName + "]; expected [htable] field type");
        }
        if (fieldType.valueType() != HtableFieldMapper.ValueType.BYTE) {
            throw new IllegalStateException("Only [byte] value type is supported; [" + fieldType.valueType().toString().toLowerCase() +  "] found");
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
            return this.defaultValue;
        }

        BytesRef data = docValues.get(docId);
        if (data == null || data.length == 0) {
            return this.defaultValue;
        }

        HashTable.Reader htableReader = fieldType.hashTableReader(data);
        int valueOffset = htableReader.getValueOffset(key);
        if (valueOffset == HashTable.Reader.NOT_FOUND_OFFSET) {
            return this.defaultValue;
        }
        return this.scaleTable[htableReader.getByte(valueOffset) & 0xff];
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

            Object minValueParam = params.get("min_value");
            if (minValueParam == null) {
                throw new ScriptException("Missing the [min_value] parameter");
            }
            double minValue = params == null ? null : XContentMapValues.nodeDoubleValue(minValueParam);

            Object maxValueParam = params.get("max_value");
            if (maxValueParam == null) {
                throw new ScriptException("Missing the [max_value] parameter");
            }
            double maxValue = params == null ? null : XContentMapValues.nodeDoubleValue(maxValueParam);

            float[] scaleTable = new float[256];
            double step = (maxValue - minValue) / (scaleTable.length - 1);
            double currentValue = minValue;
            for (int i = 0; i < scaleTable.length; i++) {
                scaleTable[i] = (float) currentValue;
                currentValue += step;
            }

            return new HtableGetScaleScript(fieldName, key, defaultValue, scaleTable);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
