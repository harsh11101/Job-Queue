package com.github.pants.jobqueue.util;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Centralized JSON serialization helper backed by the application's Jackson 3 mapper.
 * Jackson 3 exceptions are unchecked, so failures propagate as {@code JacksonException}.
 */
@Component
@RequiredArgsConstructor
public class JsonSupport {

    private final ObjectMapper objectMapper;

    /**
     * Serializes a value to compact JSON.
     *
     * @param value value to serialize.
     * @return serialized JSON string.
     */
    public String write(final Object value) {
        return objectMapper.writeValueAsString(value);
    }

    /**
     * Reads JSON as a typed value.
     *
     * @param json serialized JSON.
     * @param type target type.
     * @param <T> target type parameter.
     * @return parsed value.
     */
    public <T> T read(final String json, final Class<T> type) {
        return objectMapper.readValue(json, type);
    }

    /**
     * Converts a JSON node into canonical compact JSON.
     *
     * @param node JSON node.
     * @return canonical JSON string.
     */
    public String canonical(final JsonNode node) {
        return write(node);
    }
}
