package org.elasticsearch.script.htable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugin.mapper.MapperHtablePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSortValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasScore;
import static org.hamcrest.Matchers.containsString;


@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class HtableScriptTests extends ESIntegTestCase {
    @Override
    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(SETTING_NUMBER_OF_SHARDS, 1);
        builder.put(SETTING_NUMBER_OF_REPLICAS, 0);
        return builder.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MapperHtablePlugin.class);
    }

    private String typeMapping(String valueType, String format) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder
            .startObject().startObject("type")
                .startObject("properties")
                    .startObject("title")
                        .field("type", "string")
                    .endObject()
                    .startObject("ranks")
                        .field("type", "htable")
                        .field("value_type", valueType);
        if (format != null) {
            builder
                        .startObject("format_params")
                            .field("format", format)
                        .endObject();
        }
        builder
                    .endObject()
                .endObject()
            .endObject().endObject();
        return builder.string();
    }

    private List<IndexRequestBuilder> floatIndexBuilders(int limit) throws IOException {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < limit; i++) {
            indexBuilders.add(client()
                              .prepareIndex("test", "type", Integer.toString(i))
                              .setSource(XContentFactory.jsonBuilder()
                                         .startObject()
                                         .field("title", "rec " + i)
                                         .startObject("ranks")
                                         .array("keys", 1, 2)
                                         .array("values", i * 1000.1f, limit - i)
                                         .endObject()
                                         .endObject()));
        }
        indexBuilders.add(client()
                          .prepareIndex("test", "type", Integer.valueOf(limit).toString())
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 100")
                                     .endObject()));
        return indexBuilders;
    }

    private List<IndexRequestBuilder> byteIndexBuilders(int limit, boolean addDocWithoutHashTable) throws IOException {
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        for (int i = 0; i < limit; i++) {
            indexBuilders.add(
                              client()
                              .prepareIndex("test", "type", Integer.toString(i))
                              .setSource(XContentFactory.jsonBuilder()
                                         .startObject()
                                         .field("title", String.format("rec %s", i))
                                         .startObject("ranks")
                                         .array("keys", 1, 2)
                                         .array("values", i, limit - i)
                                         .endObject()
                                         .endObject()));
        }
        if (addDocWithoutHashTable) {
            indexBuilders.add(client()
                              .prepareIndex("test", "type", Integer.valueOf(limit).toString())
                              .setSource(XContentFactory.jsonBuilder()
                                         .startObject()
                                         .field("title", String.format("rec %s", limit))
                                         .endObject()));
        }
        return indexBuilders;
    }

    public void testFloatChainHashTableGetScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("float", null)));

        indexRandom(true, floatIndexBuilders(100));

        Map<String, Object> params = newHashMap();
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "99", "98", "97", "96", "95", "94", "93", "92", "91", "90");
        assertSearchHit(searchResponse, 1, hasScore(99009.9f));
        assertSearchHit(searchResponse, 2, hasScore(98009.8f));
        assertSearchHit(searchResponse, 10, hasScore(90009.0f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertSearchHit(searchResponse, 1, hasScore(100.0f));
        assertSearchHit(searchResponse, 2, hasScore(99.0f));
        assertSearchHit(searchResponse, 10, hasScore(91.0f));
    }

    public void testByteChainHashTableGetScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("byte", null)));

        indexRandom(true, byteIndexBuilders(255, true));

        Map<String, Object> params;
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "254", "253", "252", "251", "250", "249", "248", "247", "246", "245");
        assertSearchHit(searchResponse, 1, hasScore(254.0f));
        assertSearchHit(searchResponse, 2, hasScore(253.0f));
        assertSearchHit(searchResponse, 10, hasScore(245.0f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertSearchHit(searchResponse, 1, hasScore(255.0f));
        assertSearchHit(searchResponse, 2, hasScore(254.0f));
        assertSearchHit(searchResponse, 10, hasScore(246.0f));
    }

    public void testByteChainHashTableGetScaleScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("byte", null)));

        indexRandom(true, byteIndexBuilders(256, false));

        Map<String, Object> params;
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "255", "254", "253", "252", "251", "250", "249", "248", "247", "246");
        assertSearchHit(searchResponse, 1, hasScore(1.5f));
        assertSearchHit(searchResponse, 2, hasScore(1.497451f));
        assertSearchHit(searchResponse, 10, hasScore(1.4770588f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        assertSearchHit(searchResponse, 1, hasScore(1.5f));
        assertSearchHit(searchResponse, 2, hasScore(1.497451f));
        assertSearchHit(searchResponse, 10, hasScore(1.4770588f));
    }

    public void testFloatTrieHashTableGetScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("float", "trie")));

        indexRandom(true, floatIndexBuilders(100));

        Map<String, Object> params = newHashMap();
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "99", "98", "97", "96", "95", "94", "93", "92", "91", "90");
        assertSearchHit(searchResponse, 1, hasScore(99009.9f));
        assertSearchHit(searchResponse, 2, hasScore(98009.8f));
        assertSearchHit(searchResponse, 10, hasScore(90009.0f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertSearchHit(searchResponse, 1, hasScore(100.0f));
        assertSearchHit(searchResponse, 2, hasScore(99.0f));
        assertSearchHit(searchResponse, 10, hasScore(91.0f));
    }

    public void testByteTrieHashTableGetScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("byte", "trie")));

        indexRandom(true, byteIndexBuilders(100, true));

        Map<String, Object> params;
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "99", "98", "97", "96", "95", "94", "93", "92", "91", "90");
        assertSearchHit(searchResponse, 1, hasScore(99.0f));
        assertSearchHit(searchResponse, 2, hasScore(98.0f));
        assertSearchHit(searchResponse, 10, hasScore(90.0f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 101);

        assertOrderedSearchHits(searchResponse, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertSearchHit(searchResponse, 1, hasScore(100.0f));
        assertSearchHit(searchResponse, 2, hasScore(99.0f));
        assertSearchHit(searchResponse, 10, hasScore(91.0f));
    }

    public void testByteTrieHashTableGetScaleScript() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("byte", "trie")));

        indexRandom(true, byteIndexBuilders(256, false));

        Map<String, Object> params;
        SearchResponse searchResponse;

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "255", "254", "253", "252", "251", "250", "249", "248", "247", "246");
        assertSearchHit(searchResponse, 1, hasScore(1.5f));
        assertSearchHit(searchResponse, 2, hasScore(1.497451f));
        assertSearchHit(searchResponse, 10, hasScore(1.4770588f));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 2);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))))
            .addField("name")
            .setSize(10)
            .execute().actionGet();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 256);

        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        assertSearchHit(searchResponse, 1, hasScore(1.5f));
        assertSearchHit(searchResponse, 2, hasScore(1.497451f));
        assertSearchHit(searchResponse, 10, hasScore(1.4770588f));
    }

    public void testHashTableGetScriptMissingParams() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("float", null)));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client()
                          .prepareIndex("test", "type", "0")
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 0")
                                     .endObject()));
        indexRandom(true, indexBuilders);

        Map<String, Object> params;
        SearchRequestBuilder searchRequestBuilder;

        params = newHashMap();
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [field] parameter]"));

        params = newHashMap();
        params.put("field", "ranks");
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [key] parameter]"));
    }

    public void testHtableGetScriptNoMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type")
                .startObject("properties")
                    .startObject("title")
                        .field("type", "string")
                    .endObject()
                .endObject()
            .endObject().endObject()
            .string();

        assertAcked(prepareCreate("test").addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client()
                          .prepareIndex("test", "type", "0")
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 0")
                                     .endObject()));
        indexRandom(true, indexBuilders);

        Map<String, Object> params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[No field found for [ranks]; expected [htable] field type]"));
    }

    public void testByteHashTableGetScaleScriptIncorrectValueType() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("float", null)));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client()
                          .prepareIndex("test", "type", "0")
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 0")
                                     .endObject()));
        indexRandom(true, indexBuilders);

        Map<String, Object> params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Only [byte] value type is supported; [float] found]"));
    }

    public void testByteHtableGetScaleScriptNoMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject().startObject("type")
                .startObject("properties")
                    .startObject("title")
                        .field("type", "string")
                    .endObject()
                .endObject()
            .endObject().endObject()
            .string();

        assertAcked(prepareCreate("test").addMapping("type", mapping));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client()
                          .prepareIndex("test", "type", "0")
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 0")
                                     .endObject()));
        indexRandom(true, indexBuilders);

        Map<String, Object> params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        params.put("default_value", 0.85f);
        params.put("min_value", 0.85f);
        params.put("max_value", 1.5f);
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[No field found for [ranks]; expected [htable] field type]"));
    }

    public void testHtableGetScaleScriptMissingParams() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", typeMapping("byte", null)));

        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        indexBuilders.add(client()
                          .prepareIndex("test", "type", "0")
                          .setSource(XContentFactory.jsonBuilder()
                                     .startObject()
                                     .field("title", "rec 0")
                                     .endObject()));
        indexRandom(true, indexBuilders);

        Map<String, Object> params;
        SearchRequestBuilder searchRequestBuilder;

        params = newHashMap();
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [field] parameter]"));

        params = newHashMap();
        params.put("field", "ranks");
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [key] parameter]"));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [min_value] parameter]"));

        params = newHashMap();
        params.put("field", "ranks");
        params.put("key", 1);
        params.put("min_value", 0.5);
        searchRequestBuilder = client().prepareSearch("test")
            .setQuery(functionScoreQuery(scriptFunction(new Script("htable_get_scale", ScriptService.ScriptType.INLINE, "htable", params))));

        assertFailures(searchRequestBuilder,
                       RestStatus.INTERNAL_SERVER_ERROR,
                       containsString("[Missing the [max_value] parameter]"));
    }
}
