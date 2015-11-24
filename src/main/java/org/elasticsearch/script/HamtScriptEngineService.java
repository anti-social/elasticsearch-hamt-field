package org.elasticsearch.script;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.NativeScriptEngineService;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.hamt.AbstractHamtSearchScript;
import org.elasticsearch.script.hamt.HamtGetScript;
import org.elasticsearch.script.hamt.HamtGetScaleScript;
import org.elasticsearch.search.lookup.SearchLookup;


public class HamtScriptEngineService extends NativeScriptEngineService {
    public static final String NAME = "hamt";

    private static final ImmutableMap<String, NativeScriptFactory> SCRIPTS =
        ImmutableMap.of("hamt_get", (NativeScriptFactory) new HamtGetScript.Factory(),
                        "get", (NativeScriptFactory) new HamtGetScript.Factory(),
                        "hamt_get_scale", (NativeScriptFactory) new HamtGetScaleScript.Factory(),
                        "get_scale", (NativeScriptFactory) new HamtGetScaleScript.Factory());

    @Inject
    public HamtScriptEngineService(Settings settings) {
        super(settings, SCRIPTS);
    }

    @Override
    public String[] types() {
        return new String[]{NAME};
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        final NativeScriptFactory scriptFactory = (NativeScriptFactory) compiledScript.compiled();
        return new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                AbstractHamtSearchScript script = (AbstractHamtSearchScript) scriptFactory.newScript(vars);
                script.setLookup(lookup.getLeafSearchLookup(context));
                // We need leaf reader instance to get data from lucene
                script.setLeafReader(context.reader());
                return script;
            }

            @Override
            public boolean needsScores() {
                return scriptFactory.needsScores();
            }
        };
    }
}
