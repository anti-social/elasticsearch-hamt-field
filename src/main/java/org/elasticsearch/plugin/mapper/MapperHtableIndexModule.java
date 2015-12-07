package org.elasticsearch.plugin.mapper;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.mapper.htable.RegisterHtableFieldMapper;


public class MapperHtableIndexModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(RegisterHtableFieldMapper.class).asEagerSingleton();
    }
}
