package cl.duoc.ventas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cl.duoc.ventas.modelo.TipoError;
import cl.duoc.ventas.modelo.Venta;
import cl.duoc.ventas.validacion.ValidadorVenta;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ValidadorVentaTest {

    static final Instant AHORA = Instant.parse("2026-11-19T15:00:00Z");
    static final ValidadorVenta V = new ValidadorVenta(Duration.ofSeconds(300));

    static String venta(String eventId, String tienda, String eventTime, Object cantidad, Object precio, Object total) {
        StringBuilder sb = new StringBuilder("{");
        if (eventId != null) sb.append("\"eventId\":\"").append(eventId).append("\",");
        if (tienda != null) sb.append("\"tiendaId\":\"").append(tienda).append("\",");
        if (eventTime != null) sb.append("\"eventTime\":\"").append(eventTime).append("\",");
        if (cantidad != null) sb.append("\"cantidad\":").append(cantidad).append(',');
        if (precio != null) sb.append("\"precioUnitario\":").append(precio).append(',');
        if (total != null) sb.append("\"total\":").append(total).append(',');
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    static Stream<Arguments> rechazos() {
        String t = "2026-11-19T14:59:30Z";
        return Stream.of(
                Arguments.of("JSON truncado", "{\"eventId\":\"e1\",\"tiendaId\":", TipoError.JSON_MALFORMADO),
                Arguments.of("no es objeto (array)", "[1,2,3]", TipoError.JSON_MALFORMADO),
                Arguments.of("valor vacio", "", TipoError.JSON_MALFORMADO),
                Arguments.of("sin eventId", venta(null, "T01", t, 1, 1000, 1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("sin tiendaId", venta("e1", null, t, 1, 1000, 1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("sin eventTime", venta("e1", "T01", null, 1, 1000, 1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("eventTime no ISO-8601", venta("e1", "T01", "19/11/2026 15:00", 1, 1000, 1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("cantidad como texto", venta("e1", "T01", t, "\"dos\"", 1000, 2000), TipoError.CAMPO_INVALIDO),
                Arguments.of("cantidad negativa", venta("e1", "T01", t, -1, 1000, -1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("cantidad cero", venta("e1", "T01", t, 0, 1000, 0), TipoError.CAMPO_INVALIDO),
                Arguments.of("precio decimal", venta("e1", "T01", t, 1, 1000.5, 1000), TipoError.CAMPO_INVALIDO),
                Arguments.of("sin total", venta("e1", "T01", t, 1, 1000, null), TipoError.CAMPO_INVALIDO),
                Arguments.of("total distinto", venta("e1", "T01", t, 2, 1000, 1999), TipoError.REGLA_NEGOCIO),
                Arguments.of("eventTime 1 h en el futuro", venta("e1", "T01", "2026-11-19T16:00:00Z", 1, 1000, 1000), TipoError.REGLA_NEGOCIO));
    }

    @ParameterizedTest(name = "{0} -> {2}")
    @MethodSource("rechazos")
    @DisplayName("Rechazos hacia la DLQ con su tipo")
    void rechazos(String nombre, String json, TipoError esperado) {
        ValidadorVenta.Resultado r = V.validar("T01", json, AHORA);
        assertFalse(r.valido(), nombre);
        assertEquals(esperado, r.tipoError(), nombre + ": " + r.motivo());
    }

    @Test
    @DisplayName("La key Kafka debe coincidir con tiendaId -> REGLA_NEGOCIO")
    void keyDistinta() {
        ValidadorVenta.Resultado r = V.validar("T02", venta("e1", "T01", "2026-11-19T14:59:30Z", 1, 1000, 1000), AHORA);
        assertEquals(TipoError.REGLA_NEGOCIO, r.tipoError());
        assertEquals("e1", r.eventId());
    }

    @Test
    @DisplayName("Una venta correcta se convierte al DTO tipado")
    void ventaCorrecta() {
        String json = "{\"eventId\":\"e1\",\"tiendaId\":\"T01\",\"tiendaNombre\":\"Centro\",\"eventTime\":\"2026-11-19T14:59:30-03:00\","
                + "\"clienteId\":\"c1\",\"cantidad\":3,\"precioUnitario\":1990,\"total\":5970,\"extra\":true}";
        ValidadorVenta.Resultado r = V.validar("T01", json, Instant.parse("2026-11-19T18:00:00Z"));
        assertTrue(r.valido(), r.motivo());
        Venta v = r.venta();
        assertEquals("e1", v.eventId());
        assertEquals(Instant.parse("2026-11-19T17:59:30Z"), v.eventTime());
        assertEquals(3, v.cantidad());
        assertEquals(5970, v.total());
        assertEquals("Centro", v.tiendaNombre());
    }

    @Test
    @DisplayName("Un evento antiguo NO es invalido (lo decide la ventana, no el validador)")
    void eventoAntiguoEsValido() {
        assertTrue(V.validar("T01", venta("e1", "T01", "2026-11-17T10:00:00Z", 1, 1000, 1000), AHORA).valido());
    }

    @Test
    @DisplayName("Config rechaza retencion de dedup < ventana + gracia, topics repetidos, RF 3 y bootstrap ausente")
    void configInvalida() {
        Map<String, String> base = new HashMap<>(Map.of("KAFKA_BOOTSTRAP_SERVERS", "k:9092"));
        Config ok = Config.desde(base);
        assertEquals(60, ok.ventana().toSeconds());
        assertEquals(30, ok.gracia().toSeconds());

        Map<String, String> dedupCorta = new HashMap<>(base);
        dedupCorta.put("DEDUP_RETENCION_SEGUNDOS", "60");
        assertThrows(IllegalArgumentException.class, () -> Config.desde(dedupCorta));

        Map<String, String> repetidos = new HashMap<>(base);
        repetidos.put("TOPIC_DLQ", "ventas-raw");
        assertThrows(IllegalArgumentException.class, () -> Config.desde(repetidos));

        Map<String, String> rf3 = new HashMap<>(base);
        rf3.put("REPLICATION_FACTOR", "3");
        assertThrows(IllegalArgumentException.class, () -> Config.desde(rf3));

        assertThrows(IllegalArgumentException.class, () -> Config.desde(Map.of()));
    }
}
