package cl.duoc.ventas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/** ObjectMapper compartido y Serde JSON tipado para los state stores. */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }

    public static String escribir(Object o) {
        try {
            return MAPPER.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar " + o, e);
        }
    }

    public static <T> T leer(String json, Class<T> tipo) {
        try {
            return MAPPER.readValue(json, tipo);
        } catch (IOException e) {
            throw new IllegalArgumentException("JSON invalido para " + tipo.getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    public static <T> Serde<T> serde(Class<T> tipo) {
        Serializer<T> ser = (topic, data) -> data == null ? null : escribir(data).getBytes(StandardCharsets.UTF_8);
        Deserializer<T> des = (topic, bytes) -> bytes == null ? null : leer(new String(bytes, StandardCharsets.UTF_8), tipo);
        return Serdes.serdeFrom(ser, des);
    }
}
