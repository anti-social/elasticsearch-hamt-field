package org.elasticsearch.index.mapper.hamt;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;


public class RegisterHamtFieldMapper extends AbstractIndexComponent {
    @Inject
    public RegisterHamtFieldMapper(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings);

        mapperService.documentMapperParser().putTypeParser("hamt", new HamtFieldMapper.TypeParser());
    }
}
