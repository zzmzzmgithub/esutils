package com.wirktop.esutils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * @author Cosmin Marginean
 */
public class Json {

    private static final Logger log = LoggerFactory.getLogger(Json.class);

    private ObjectMapper objectMapper;

    public Json(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <T> String toString(T pojo) {
        try {
            StringWriter writer = new StringWriter();
            objectMapper.writer().writeValue(writer, pojo);
            return writer.toString();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }

    public <T> T toPojo(String jsonString, Class<T> pojoClass) {
        try {
            return objectMapper.readValue(jsonString, pojoClass);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new SearchException(e.getMessage(), e);
        }
    }
}
