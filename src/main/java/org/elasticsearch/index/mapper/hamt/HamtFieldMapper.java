package org.elasticsearch.index.mapper.hamt;

import hamt.HAMT;

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
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;


public class HamtFieldMapper extends FieldMapper {
    private final HAMT.Writer hamtWriter;
    private final ValueParser valueParser;

    public static final String CONTENT_TYPE = "hamt";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new HamtFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.freeze();
        }

        public static ValueType VALUE_TYPE = ValueType.FLOAT;
        public static HAMT.BitmaskSize BITMASK_SIZE = HAMT.BitmaskSize.SHORT;
    }

    public static enum ValueType {
        BYTE(HAMT.ValueSize.BYTE) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return hamtReader.getByte(valueOffset) & 0xff;
            }
        },
        SHORT(HAMT.ValueSize.SHORT) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return hamtReader.getShort(valueOffset)  & 0xffff;
            }
        },
        INT(HAMT.ValueSize.INT) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return hamtReader.getInt(valueOffset);
            }
        },
        LONG(HAMT.ValueSize.LONG) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return hamtReader.getLong(valueOffset);
            }
        },
        FLOAT(HAMT.ValueSize.INT) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return hamtReader.getFloat(valueOffset);
            }
        },
        DOUBLE(HAMT.ValueSize.LONG) {
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
            public float getValue(HAMT.Reader hamtReader, int valueOffset) {
                return (float) (hamtReader.getDouble(valueOffset));
            }
        };

        public final HAMT.ValueSize valueSize;

        ValueType(HAMT.ValueSize valueSize) {
            this.valueSize = valueSize;
        }

        public abstract ValueParser parser();

        public abstract float getValue(HAMT.Reader hamtReader, int valueOffset);
    }

    interface ValueParser {
        byte[] parseValue(XContentParser parser) throws IOException;
    }

    public static class Builder extends FieldMapper.Builder<Builder, HamtFieldMapper> {
        private ValueType valueType = ValueType.FLOAT;
        private HAMT.BitmaskSize bitmaskSize = HAMT.BitmaskSize.SHORT;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder valueType(ValueType valueType) {
            this.valueType = valueType;
            return this;
        }

        public Builder bitmaskSize(HAMT.BitmaskSize bitmaskSize) {
            this.bitmaskSize = bitmaskSize;
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
        public HamtFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            ((HamtFieldType) fieldType).setValueType(valueType);
            ((HamtFieldType) fieldType).setBitmaskSize(bitmaskSize);
            return new HamtFieldMapper(name,
                                       fieldType,
                                       defaultFieldType,
                                       new HAMT.Writer(bitmaskSize, valueType.valueSize),
                                       valueType.parser(),
                                       context.indexSettings(),
                                       multiFieldsBuilder.build(this, context),
                                       copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            HamtFieldMapper.Builder builder = new HamtFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = Strings.toUnderscoreCase(entry.getKey());
                final Object propNode = entry.getValue();
                if (propName.equals("value_type")) {
                    builder.valueType(ValueType.valueOf(propNode.toString().toUpperCase()));
                    iterator.remove();
                } else if (propName.equals("bitmask_size")) {
                    builder.bitmaskSize(HAMT.BitmaskSize.valueOf(propNode.toString().toUpperCase()));
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

    public static final class HamtFieldType extends MappedFieldType {
        private ValueType valueType;
        private HAMT.BitmaskSize bitmaskSize;

        public HamtFieldType() {}

        protected HamtFieldType(HamtFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new HamtFieldType(this);
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

        public void setBitmaskSize(HAMT.BitmaskSize bitmaskSize) {
            this.bitmaskSize = bitmaskSize;
        }

        public HAMT.BitmaskSize bitmaskSize() {
            return bitmaskSize;
        }
    }

    protected HamtFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                              HAMT.Writer hamtWriter, ValueParser valueParser,
                              Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.hamtWriter = hamtWriter;
        this.valueParser = valueParser;
    }

    @Override
    public HamtFieldType fieldType() {
        return (HamtFieldType) super.fieldType();
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

            value = this.hamtWriter.dump(entries);
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
        } else if (includeDefaults || fieldType().bitmaskSize() != Defaults.BITMASK_SIZE) {
            builder.field("bitmask_size", fieldType().bitmaskSize().toString().toLowerCase());
        }
    }
}
