package org.elasticsearch.index.mapper.htable;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsService;


public class RegisterHtableFieldMapper extends AbstractIndexComponent {
    @Inject
    public RegisterHtableFieldMapper(Index index, IndexSettingsService indexSettingsService, MapperService mapperService) {
        super(index, indexSettingsService.getSettings());

        mapperService.documentMapperParser().putTypeParser("htable", new HtableFieldMapper.TypeParser());
    }
}
