package com.wirktop.esutils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * @author Cosmin Marginean
 */
public class InstantDeserializer extends JsonDeserializer<Instant> {

    @Override
    public Instant deserialize(JsonParser jsonparser, DeserializationContext deserializationcontext) throws IOException {
        return Instant.from(DateTimeFormatter.ISO_INSTANT.parse(jsonparser.getText()));
    }
}
