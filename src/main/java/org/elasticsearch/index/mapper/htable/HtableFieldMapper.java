package org.elasticsearch.index.mapper.htable;

import net.uaprom.htable.HashTable;
import net.uaprom.htable.ChainHashTable;
import net.uaprom.htable.TrieHashTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;


public class HtableFieldMapper extends FieldMapper {
    private final HashTable.Writer htableWriter;
    private final ValueParser valueParser;

    public static final String CONTENT_TYPE = "htable";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new HtableFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }

        public static ValueType VALUE_TYPE = ValueType.FLOAT;
        public static String FORMAT = "chain";
    }

    public static enum ValueType {
        BYTE(HashTable.ValueSize.BYTE) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        return new byte[]{ (byte) (parser.shortValue()) };
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return htableReader.getByte(valueOffset) & 0xff;
            }
        },
        SHORT(HashTable.ValueSize.SHORT) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
                        return buffer.putShort(parser.shortValue()).array();
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return htableReader.getShort(valueOffset)  & 0xffff;
            }
        },
        INT(HashTable.ValueSize.INT) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                        return buffer.putInt(parser.intValue()).array();
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return htableReader.getInt(valueOffset);
            }
        },
        LONG(HashTable.ValueSize.LONG) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                        return buffer.putLong(parser.longValue()).array();
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return htableReader.getLong(valueOffset);
            }
        },
        FLOAT(HashTable.ValueSize.INT) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                        return buffer.putFloat(parser.floatValue()).array();
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return htableReader.getFloat(valueOffset);
            }
        },
        DOUBLE(HashTable.ValueSize.LONG) {
            @Override
            public ValueParser parser() {
                return new ValueParser() {
                    @Override
                    public byte[] parseValue(XContentParser parser) throws IOException {
                        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                        return buffer.putDouble(parser.doubleValue()).array();
                    }
                };
            }

            @Override
            public float getValue(HashTable.Reader htableReader, int valueOffset) {
                return (float) (htableReader.getDouble(valueOffset));
            }
        };

        public final HashTable.ValueSize valueSize;

        ValueType(HashTable.ValueSize valueSize) {
            this.valueSize = valueSize;
        }

        public abstract ValueParser parser();

        public abstract float getValue(HashTable.Reader htableReader, int valueOffset);
    }

    interface ValueParser {
        byte[] parseValue(XContentParser parser) throws IOException;
    }

    public static class Builder extends FieldMapper.Builder<Builder, HtableFieldMapper> {
        private ValueType valueType = ValueType.FLOAT;
        private Map<String, Object> dataFormatParams = null;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder valueType(ValueType valueType) {
            this.valueType = valueType;
            return this;
        }

        public Builder dataFormatParams(Map<String, Object> dataFormatParams) {
            this.dataFormatParams = dataFormatParams;
            return this;
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType.setIndexOptions(IndexOptions.NONE);
            defaultFieldType.setIndexOptions(IndexOptions.NONE);
            fieldType.setHasDocValues(true);
            defaultFieldType.setHasDocValues(true);
        }

        @Override
        public HtableFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ((HtableFieldType) fieldType).setValueType(valueType);
            ((HtableFieldType) fieldType).setDataFormatParams(dataFormatParams);
            HashTable.Writer htableWriter = null;
            if (dataFormatParams == null) {
                htableWriter = new ChainHashTable.Writer(valueType.valueSize);
            } else {
                String format = XContentMapValues.nodeStringValue(dataFormatParams.get("format"), Defaults.FORMAT);
                if (format.equals("chain")) {
                    int fillingRatio = XContentMapValues.nodeIntegerValue(dataFormatParams.get("filling_ratio"), ChainHashTable.Writer.DEFAULT_FILLING_RATIO);
                    int minHashTableSize = XContentMapValues.nodeIntegerValue(dataFormatParams.get("min_hash_table_size"), ChainHashTable.Writer.DEFAULT_MIN_HASH_TABLE_SIZE);
                    htableWriter = new ChainHashTable.Writer(valueType.valueSize, fillingRatio, minHashTableSize);
                } else if (format.equals("trie")) {
                    TrieHashTable.BitmaskSize bitmaskSize;
                    String bitmaskSizeParam = XContentMapValues.nodeStringValue(dataFormatParams.get("bitmask_size"), null);
                    if (bitmaskSizeParam != null) {
                        bitmaskSize = TrieHashTable.BitmaskSize.valueOf(bitmaskSizeParam.toUpperCase());
                    } else {
                        bitmaskSize = TrieHashTable.BitmaskSize.SHORT;
                    }
                    htableWriter = new TrieHashTable.Writer(valueType.valueSize, bitmaskSize);
                }
            }
            return new HtableFieldMapper(name,
                                         fieldType,
                                         defaultFieldType,
                                         htableWriter,
                                         valueType.parser(),
                                         context.indexSettings(),
                                         multiFieldsBuilder.build(this, context),
                                         copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            HtableFieldMapper.Builder builder = new HtableFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = Strings.toUnderscoreCase(entry.getKey());
                final Object propNode = entry.getValue();
                if (propName.equals("value_type")) {
                    builder.valueType(ValueType.valueOf(propNode.toString().toUpperCase()));
                    iterator.remove();
                } else if (propName.equals("format_params")) {
                    Map<String, Object> dataFormatParams = (Map<String, Object>) propNode;
                    builder.dataFormatParams(dataFormatParams);
                    if (!dataFormatParams.containsKey("format")) {
                        throw new MapperParsingException("[format] can be [chain] or [trie]");
                    }
                    iterator.remove();
                } else if (propName.equals("index")) {
                    throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
                } else if (propName.equals("doc_values")) {
                    throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
                }
            }
            return builder;
        }
    }

    public static final class HtableFieldType extends MappedFieldType {
        private ValueType valueType;
        Map<String, Object> dataFormatParams;

        public HtableFieldType() {}

        protected HtableFieldType(HtableFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new HtableFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public void setValueType(ValueType valueType) {
            this.valueType = valueType;
        }

        public ValueType valueType() {
            return valueType;
        }

        public void setDataFormatParams(Map<String, Object> dataFormatParams) {
            this.dataFormatParams = dataFormatParams;
        }

        public Map<String, Object> dataFormatParams() {
            return dataFormatParams;
        }

        public HashTable.Reader hashTableReader(BytesRef data) {
            String format = dataFormatParams == null ? Defaults.FORMAT : XContentMapValues.nodeStringValue(dataFormatParams.get("format"), Defaults.FORMAT);
            if (format.equals("trie")) {
                return new TrieHashTable.Reader(data.bytes, data.offset, data.length);
            }
            return new ChainHashTable.Reader(data.bytes, data.offset, data.length);
        }
    }

    protected HtableFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                HashTable.Writer htableWriter, ValueParser valueParser,
                                Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.htableWriter = htableWriter;
        this.valueParser = valueParser;
    }

    @Override
    public HtableFieldType fieldType() {
        return (HtableFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private List<Long> parseKeys(XContentParser parser) throws IOException {
        List<Long> keys = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (token != XContentParser.Token.END_ARRAY) {
                if(parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    keys.add(parser.longValue());
                }
                token = parser.nextToken();
            }
        }
        return keys;
    }

    private List<byte[]> parseValues(XContentParser parser) throws IOException {
        List<byte[]> values = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (token != XContentParser.Token.END_ARRAY) {
                if(parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    values.add(this.valueParser.parseValue(parser));
                }
                token = parser.nextToken();
            }
        }
        return values;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        byte[] value = context.parseExternalValue(byte[].class);

        if (value == null) {
            List<Long> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();

            XContentParser.Token token = context.parser().currentToken();
            // its an object of keys and values { "keys": [1, 2, 3], "values": [1.4, 1.5, 1.6] }
            if (token == XContentParser.Token.START_OBJECT) {
                token = context.parser().nextToken();
                while (token != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = context.parser().currentName();
                        if (fieldName.equals("keys")) {
                            keys = parseKeys(context.parser());
                        } else if (fieldName.equals("values")) {
                            values = parseValues(context.parser());
                        }
                    }
                    token = context.parser().nextToken();
                }
            }
            // TODO: add support for an array of keys and values [ [1, 2, 3], [1.4, 1.5, 1.6] ]
            // else if (token == XContentParser.Token.START_ARRAY) {
            //     keys = parseKeys(context.parser());
            //     values = parseValues(context.parser());
            // }

            if (keys.size() != values.size()) {
                throw new MapperParsingException("'keys' and 'length' have different size.");
            }

            SortedMap<Long, byte[]> entries = new TreeMap<>();
            for (int i = 0; i < keys.size(); i++) {
                entries.put(keys.get(i), values.get(i));
            }

            value = this.htableWriter.dump(entries);
        }

        if (value == null) {
            return;
        }

        if (fieldType().stored()) {
            fields.add(new Field(fieldType().names().indexName(), value, fieldType()));
        }

        if (fieldType().hasDocValues()) {
            fields.add(new org.apache.lucene.document.BinaryDocValuesField(fieldType().names().indexName(), new BytesRef(value)));
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().valueType() != Defaults.VALUE_TYPE) {
            builder.field("value_type", fieldType().valueType().toString().toLowerCase());
        }
        if (fieldType().dataFormatParams() != null) {
            builder.field("format_params", fieldType().dataFormatParams());
        }
    }
}
