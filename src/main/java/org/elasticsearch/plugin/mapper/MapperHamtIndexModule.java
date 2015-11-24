package org.elasticsearch.plugin.mapper;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.mapper.hamt.RegisterHamtFieldMapper;


public class MapperHamtIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RegisterHamtFieldMapper.class).asEagerSingleton();
    }
}
