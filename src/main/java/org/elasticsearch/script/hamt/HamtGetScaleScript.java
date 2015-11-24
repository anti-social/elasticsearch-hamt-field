package org.elasticsearch.script.hamt;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import hamt.HAMT;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.hamt.HamtFieldMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;


public class HamtGetScaleScript extends AbstractHamtSearchScript {
    private final byte defaultValue;

    private final float[] scaleTable;

    private HamtGetScaleScript(String fieldName, long key, byte defaultValue, float[] scaleTable) {
        super(fieldName, key);
        this.defaultValue = defaultValue;
        this.scaleTable = scaleTable;
    }

    @Override
    public float runAsFloat() {
        HamtFieldMapper.HamtFieldType fieldType = (HamtFieldMapper.HamtFieldType) (doc().mapperService().smartNameFieldType(fieldName));
        if (fieldType.valueType() != HamtFieldMapper.ValueType.BYTE) {
            throw new IllegalStateException("Only 'byte' value type is supported");
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

        BytesRef data = docValues.get(docId);
        if (data == null || data.length == 0) {
            return this.scaleTable[defaultValue & 0xff];
        }

        HAMT.Reader hamtReader = new HAMT.Reader(data.bytes);
        int valueOffset = hamtReader.getValueOffset(key);
        if (valueOffset == HAMT.Reader.NOT_FOUND_OFFSET) {
            return this.scaleTable[defaultValue & 0xff];
        }
        return this.scaleTable[hamtReader.getByte(valueOffset) & 0xff];
    }

    public static class Factory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
            if (fieldName == null) {
                throw new ScriptException("Missing the 'field' parameter");
            }

            Object keyParam = params.get("key");
            if (keyParam == null) {
                throw new ScriptException("Missing the 'key' parameter");
            }
            long key = params == null ? null : XContentMapValues.nodeLongValue(keyParam);
            
            byte defaultValue = params == null ? null : XContentMapValues.nodeByteValue(params.get("default"), (byte) 0);

            Object minValueParam = params.get("min_value");
            if (minValueParam == null) {
                throw new ScriptException("Missing the 'min_value' parameter");
            }
            double minValue = params == null ? null : XContentMapValues.nodeDoubleValue(minValueParam);

            Object maxValueParam = params.get("max_value");
            if (maxValueParam == null) {
                throw new ScriptException("Missing the 'max_value' parameter");
            }
            double maxValue = params == null ? null : XContentMapValues.nodeDoubleValue(maxValueParam);

            float[] scaleTable = new float[256];
            double step = (maxValue - minValue) / (scaleTable.length - 1);
            double currentValue = minValue;
            for (int i = 0; i < scaleTable.length; i++) {
                scaleTable[i] = (float) currentValue;
                currentValue += step;
            }

            return new HamtGetScaleScript(fieldName, key, defaultValue, scaleTable);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
