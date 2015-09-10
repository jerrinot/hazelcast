package com.hazelcast.query.impl.getters;

import uk.co.rockstable.experiements.codegen.reflection.extractors.Extractor;
import uk.co.rockstable.experiements.codegen.reflection.extractors.ExtractorFactory;

import java.lang.reflect.Field;

public class MagicGetter extends Getter {
    private final Field field;
    private static final ExtractorFactory extractorFactory = ExtractorFactory.newInstance(ExtractorFactory.Type.MAGIC);
    private final Extractor extractor;

    public MagicGetter(Getter parent, Field field) {
        super(parent);
        this.field = field;

        Class actualType;
        if (parent == null) {
            actualType = field.getDeclaringClass();
        } else {
            actualType = parent.getReturnType();
        }

        String fieldName = field.getName();
        extractor = extractorFactory.create(actualType, fieldName);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object paramObj = obj;
        paramObj = parent != null ? parent.getValue(paramObj) : paramObj;
        return paramObj != null ? extractor.extract(paramObj) : null;
    }

    @Override
    Class getReturnType() {
        return this.field.getType();
    }

    @Override
    boolean isCacheable() {
        return ReflectionHelper.THIS_CL.equals(field.getDeclaringClass().getClassLoader());
    }
}
