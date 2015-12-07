package org.elasticsearch.index.mapper.htable;

import net.uaprom.htable.HashTable;
import net.uaprom.htable.TrieHashTable;
import net.uaprom.htable.ChainHashTable;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.instanceOf;


public class HtableMappingTests extends ESSingleNodeTestCase {
    public void testChainHtableByteValueMapping() throws Exception {
        Settings settings = Settings.settingsBuilder()
            .put("path.home", createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("product")
                    .startObject("properties")
                        .startObject("category_ranks")
                            .field("type", "htable")
                            .field("value_type", "byte")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        mapperParser.putTypeParser("htable", new HtableFieldMapper.TypeParser());
        DocumentMapper mapper = mapperParser.parse(mapping);

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("category_ranks");
        assertThat(fieldMapper, instanceOf(HtableFieldMapper.class));
        assertThat(fieldMapper.fieldType(), instanceOf(HtableFieldMapper.HtableFieldType.class));
        assertEquals("htable", fieldMapper.fieldType().fieldDataType().getType());

        long[] keys = new long[]{ 1L, 2L, 3L };
        byte[] values = new byte[]{ (byte) 101, (byte) 102, (byte) 103 };
        BytesRef binaryValue = new BytesRef(new ChainHashTable.Writer(HashTable.ValueSize.BYTE)
                                            .dumpBytes(keys, values));
        BytesRef indexedValue;
        XContentBuilder fieldDataBuilder;
        ParseContext.Document doc;

        fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 1L, 2L, 3L)
                    .array("values", 101, 102, 103)
                .endObject()
            .endObject();
        doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();
        indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(binaryValue, indexedValue);

        fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 3L, 2L, 1L)
                    .array("values", 103, 102, 101)
                .endObject()
            .endObject();
        doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();

        indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(binaryValue, indexedValue);
    }

    public void testChainHtableFloatValueMapping() throws Exception {
        Settings settings = Settings.settingsBuilder()
            .put("path.home", createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("product")
                    .startObject("properties")
                        .startObject("category_ranks")
                            .field("type", "htable")
                            .field("value_type", "float")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        mapperParser.putTypeParser("htable", new HtableFieldMapper.TypeParser());
        DocumentMapper mapper = mapperParser.parse(mapping);

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("category_ranks");
        assertThat(fieldMapper, instanceOf(HtableFieldMapper.class));
        assertThat(fieldMapper.fieldType(), instanceOf(HtableFieldMapper.HtableFieldType.class));
        assertEquals("htable", fieldMapper.fieldType().fieldDataType().getType());

        long[] keys = new long[]{ 1L, 2L, 3L };
        float[] values = new float[]{ 101.1f, 102.2f, 103.3f };
        byte[] binaryValue = new ChainHashTable.Writer(HashTable.ValueSize.INT).dumpFloats(keys, values);

        XContentBuilder fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 1L, 2L, 3L)
                    .array("values", 101.1, 102.2, 103.3)
                .endObject()
            .endObject();

        ParseContext.Document doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();

        BytesRef indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(new BytesRef(binaryValue), indexedValue);
    }

    public void testTrieHtableByteValueMapping() throws Exception {
        Settings settings = Settings.settingsBuilder()
            .put("path.home", createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("product")
                    .startObject("properties")
                        .startObject("category_ranks")
                            .field("type", "htable")
                            .field("value_type", "byte")
                            .startObject("params")
                                .field("format", "trie")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        mapperParser.putTypeParser("htable", new HtableFieldMapper.TypeParser());
        DocumentMapper mapper = mapperParser.parse(mapping);

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("category_ranks");
        assertThat(fieldMapper, instanceOf(HtableFieldMapper.class));
        assertThat(fieldMapper.fieldType(), instanceOf(HtableFieldMapper.HtableFieldType.class));
        assertEquals("htable", fieldMapper.fieldType().fieldDataType().getType());

        long[] keys = new long[]{ 1L, 2L, 3L };
        byte[] values = new byte[]{ (byte) 101, (byte) 102, (byte) 103 };
        BytesRef binaryValue = new BytesRef(new TrieHashTable.Writer(TrieHashTable.BitmaskSize.SHORT, HashTable.ValueSize.BYTE)
                                            .dumpBytes(keys, values));
        BytesRef indexedValue;
        XContentBuilder fieldDataBuilder;
        ParseContext.Document doc;

        fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 1L, 2L, 3L)
                    .array("values", 101, 102, 103)
                .endObject()
            .endObject();
        doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();
        indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(binaryValue, indexedValue);

        fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 3L, 2L, 1L)
                    .array("values", 103, 102, 101)
                .endObject()
            .endObject();
        doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();

        indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(binaryValue, indexedValue);
    }

    public void testTrieHtableFloatValueMapping() throws Exception {
        Settings settings = Settings.settingsBuilder()
            .put("path.home", createTempDir().toString())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("product")
                    .startObject("properties")
                        .startObject("category_ranks")
                            .field("type", "htable")
                            .field("value_type", "float")
                            .startObject("params")
                                .field("format", "trie")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .string();

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        mapperParser.putTypeParser("htable", new HtableFieldMapper.TypeParser());
        DocumentMapper mapper = mapperParser.parse(mapping);

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("category_ranks");
        assertThat(fieldMapper, instanceOf(HtableFieldMapper.class));
        assertThat(fieldMapper.fieldType(), instanceOf(HtableFieldMapper.HtableFieldType.class));
        assertEquals("htable", fieldMapper.fieldType().fieldDataType().getType());

        long[] keys = new long[]{ 1L, 2L, 3L };
        float[] values = new float[]{ 101.1f, 102.2f, 103.3f };
        byte[] binaryValue = new TrieHashTable.Writer(TrieHashTable.BitmaskSize.SHORT, HashTable.ValueSize.INT).dumpFloats(keys, values);

        XContentBuilder fieldDataBuilder = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("category_ranks")
                    .array("keys", 1L, 2L, 3L)
                    .array("values", 101.1, 102.2, 103.3)
                .endObject()
            .endObject();

        ParseContext.Document doc = mapper.parse("test", "product", "1", fieldDataBuilder.bytes()).rootDoc();

        BytesRef indexedValue = doc.getBinaryValue("category_ranks");
        assertEquals(new BytesRef(binaryValue), indexedValue);
    }

    // public void testListOfListValues() {
    //     XContentBuilder fieldDataBuilder = XContentFactory.jsonBuilder()
    //         .startObject()
    //         .startArray("category_ranks");
    //     // keys
    //     long[] keys = new long[] {1L, 2L, 3L};
    //     fieldDataBuilder.startArray();
    //     for (long k : keys) {
    //         fieldDataBuilder.value(k);
    //     }
    //     fieldDataBuilder.endArray();
    //     // values
    //     byte[] values = new byte[] {101, 102, 103};
    //     fieldDataBuilder.startArray();
    //     for (long v : values) {
    //         fieldDataBuilder.value(v);
    //     }
    //     fieldDataBuilder.endArray();
    //     fieldDataBuilder
    //         .endArray()
    //         .endObject();
    // }
}
