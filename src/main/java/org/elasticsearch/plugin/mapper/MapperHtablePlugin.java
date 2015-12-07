package org.elasticsearch.plugin.mapper;

import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.HtableScriptEngineService;
import org.elasticsearch.script.ScriptModule;


public class MapperHtablePlugin extends Plugin {
    @Override
    public String name() {
        return "mapper-htable";
    }

    @Override
    public String description() {
        return "Adds the hash table field type to store hash maps";
    }

    @Override
    public Collection<Module> indexModules(Settings indexSettings) {
        return Collections.<Module>singletonList(new MapperHtableIndexModule());
    }

    public void onModule(ScriptModule scriptModule) {
        scriptModule.addScriptEngine(HtableScriptEngineService.class);
    }
}
