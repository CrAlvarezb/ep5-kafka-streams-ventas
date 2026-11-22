package cl.duoc.ventas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cl.duoc.ventas.modelo.MotivoDescarte;
import cl.duoc.ventas.modelo.RegistroDlq;
import cl.duoc.ventas.modelo.TipoError;
import cl.duoc.ventas.modelo.VentaAgregada;
import cl.duoc.ventas.modelo.VentaDescartada;
import cl.duoc.ventas.streams.StreamsApp;
import cl.duoc.ventas.streams.TopologiaVentas;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Ejecuta la topologia real con TopologyTestDriver (sin broker). Ventana 60 s, gracia 30 s.
 * Ventana W1 = [15:00:00, 15:01:00), cierra en 15:01:30.
 */
class TopologiaTest {

    static final Instant T0 = Instant.parse("2026-11-19T15:00:00Z");

    TopologyTestDriver driver;
    TestInputTopic<String, String> raw;
    TestOutputTopic<String, String> agregadas;
    TestOutputTopic<String, String> dlq;
    TestOutputTopic<String, String> descartadas;

    @BeforeEach
    void setUp() throws Exception {
        Path dir = Files.createTempDirectory("kstreams-test");
        Config cfg = Config.desde(Map.of("KAFKA_BOOTSTRAP_SERVERS", "dummy:9092", "STATE_DIR", dir.toString()));
        Properties p = StreamsApp.propiedades(cfg);
        driver = new TopologyTestDriver(TopologiaVentas.construir(cfg), p, T0.plusSeconds(120));
        StringSerializer ser = new StringSerializer();
        StringDeserializer des = new StringDeserializer();
        raw = driver.createInputTopic(cfg.topicRaw(), ser, ser);
        agregadas = driver.createOutputTopic(cfg.topicAgregadas(), des, des);
        dlq = driver.createOutputTopic(cfg.topicDlq(), des, des);
        descartadas = driver.createOutputTopic(cfg.topicDescartadas(), des, des);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    static String venta(String eventId, String tienda, Instant t, int cantidad, long precio) {
        return String.format("{\"eventId\":\"%s\",\"tiendaId\":\"%s\",\"eventTime\":\"%s\",\"cantidad\":%d,\"precioUnitario\":%d,\"total\":%d}",
                eventId, tienda, t, cantidad, precio, cantidad * precio);
    }

    void enviar(String eventId, String tienda, int segundos, int cantidad, long precio) {
        Instant t = T0.plusSeconds(segundos);
        raw.pipeInput(tienda, venta(eventId, tienda, t, cantidad, precio), t);
    }

    /** Venta de otra tienda que adelanta el stream time y cierra las ventanas anteriores. */
    void avanzarA(int segundos) {
        enviar("avance-" + segundos, "T99", segundos, 1, 1);
    }

    List<VentaAgregada> agregadosDe(String tienda) {
        return agregadas.readValuesToList().stream()
                .map(j -> Json.leer(j, VentaAgregada.class))
                .filter(a -> a.tiendaId().equals(tienda))
                .toList();
    }

    @Test
    @DisplayName("Agregacion: conteo, monto, ticket promedio e inicio/fin de ventana; se publica solo al cerrar")
    void agregacion() {
        enviar("e1", "T01", 5, 1, 1000);
        enviar("e2", "T01", 10, 2, 1500);
        enviar("e3", "T01", 20, 1, 4000);
        avanzarA(80); // W1 cerrada por tiempo (15:01:00) pero dentro de la gracia
        assertTrue(agregadas.isEmpty(), "no debe publicar antes de fin + gracia");

        avanzarA(91); // 15:01:31 > 15:01:30 -> cierra W1
        List<VentaAgregada> r = agregadosDe("T01");
        assertEquals(1, r.size());
        VentaAgregada a = r.get(0);
        assertEquals(T0, a.windowStart());
        assertEquals(T0.plusSeconds(60), a.windowEnd());
        assertEquals(3, a.cantidadVentas());
        assertEquals(4, a.unidades());
        assertEquals(8000, a.montoTotal());
        assertEquals(2667, a.ticketPromedio());
    }

    @Test
    @DisplayName("DLQ: el invalido conserva original, key, topic, particion, offset y motivo; el flujo sigue")
    void dlqConservaOriginalYSigue() {
        String roto = "{\"eventId\":\"x1\",\"tiendaId\":\"T01\",";
        raw.pipeInput("T01", roto, T0.plusSeconds(1));
        enviar("e1", "T01", 2, 1, 5000);
        avanzarA(91);

        TestRecord<String, String> rec = dlq.readRecord();
        RegistroDlq d = Json.leer(rec.value(), RegistroDlq.class);
        assertEquals(roto, d.rawValue());
        assertEquals("T01", d.originalKey());
        assertEquals("T01", rec.key());
        assertEquals("ventas-raw", d.sourceTopic());
        assertEquals(0, d.sourcePartition());
        assertEquals(0, d.sourceOffset());
        assertEquals(TipoError.JSON_MALFORMADO, d.errorType());
        assertNotNull(d.error());
        assertEquals("JSON_MALFORMADO", new String(rec.headers().lastHeader("dlq.error.type").value(), StandardCharsets.UTF_8));
        assertTrue(dlq.isEmpty());

        List<VentaAgregada> r = agregadosDe("T01");
        assertEquals(1, r.get(0).cantidadVentas(), "la venta valida posterior se procesa igual");
    }

    @Test
    @DisplayName("DLQ: errores de campo y de negocio con su tipo")
    void dlqTipos() {
        Instant t = T0.plusSeconds(5);
        raw.pipeInput("T01", "{\"eventId\":\"c1\",\"tiendaId\":\"T01\",\"eventTime\":\"" + t + "\",\"cantidad\":\"dos\",\"precioUnitario\":10,\"total\":20}", t);
        raw.pipeInput("T01", "{\"eventId\":\"n1\",\"tiendaId\":\"T01\",\"eventTime\":\"" + t + "\",\"cantidad\":2,\"precioUnitario\":10,\"total\":21}", t);
        List<RegistroDlq> r = dlq.readValuesToList().stream().map(j -> Json.leer(j, RegistroDlq.class)).toList();
        assertEquals(TipoError.CAMPO_INVALIDO, r.get(0).errorType());
        assertEquals("c1", r.get(0).eventId());
        assertEquals(TipoError.REGLA_NEGOCIO, r.get(1).errorType());
        assertEquals(1, r.get(1).sourceOffset());
    }

    @Test
    @DisplayName("Duplicados: el mismo eventId dos veces se cuenta una sola vez y queda auditado")
    void duplicado() {
        enviar("dup-1", "T01", 5, 2, 5000);
        enviar("dup-1", "T01", 6, 2, 5000);
        enviar("e2", "T01", 7, 1, 5000);
        avanzarA(91);

        VentaDescartada d = Json.leer(descartadas.readValue(), VentaDescartada.class);
        assertEquals(MotivoDescarte.DUPLICADO, d.motivo());
        assertEquals("dup-1", d.eventId());
        assertEquals(1, d.sourceOffset());
        assertTrue(d.detalle().contains("ventas-raw-0@0"), d.detalle());

        VentaAgregada a = agregadosDe("T01").get(0);
        assertEquals(2, a.cantidadVentas());
        assertEquals(15000, a.montoTotal());
        assertTrue(dlq.isEmpty(), "un duplicado no es un error de datos");
    }

    @Test
    @DisplayName("Tiempo de evento: un tardio dentro de la gracia se suma a su ventana original")
    void tardioEnGracia() {
        enviar("e1", "T01", 50, 1, 5000);   // W1
        enviar("e2", "T01", 70, 1, 3000);   // W2, stream time 15:01:10
        enviar("tarde", "T01", 40, 1, 7000); // W1 cierra 15:01:30 > 15:01:10 -> se acepta
        avanzarA(151);

        assertTrue(descartadas.isEmpty());
        List<VentaAgregada> r = agregadosDe("T01");
        VentaAgregada w1 = r.stream().filter(a -> a.windowStart().equals(T0)).findFirst().orElseThrow();
        assertEquals(2, w1.cantidadVentas());
        assertEquals(12000, w1.montoTotal());
    }

    @Test
    @DisplayName("Tiempo de evento: un tardio fuera de gracia se descarta con registro, no va a la DLQ")
    void tardioFueraDeGracia() {
        enviar("e1", "T01", 10, 1, 5000);
        enviar("e2", "T01", 120, 1, 5000);      // stream time 15:02:00
        enviar("muy-tarde", "T01", 15, 1, 9000); // W1 cerro en 15:01:30
        avanzarA(211);

        VentaDescartada d = Json.leer(descartadas.readValue(), VentaDescartada.class);
        assertEquals(MotivoDescarte.TARDIO_FUERA_DE_GRACIA, d.motivo());
        assertEquals("muy-tarde", d.eventId());
        assertEquals(T0, d.windowStart());
        assertEquals(T0.plusSeconds(120), d.streamTime());
        assertTrue(dlq.isEmpty(), "tardio no es invalido");

        VentaAgregada w1 = agregadosDe("T01").stream().filter(a -> a.windowStart().equals(T0)).findFirst().orElseThrow();
        assertEquals(1, w1.cantidadVentas());
        assertEquals(5000, w1.montoTotal());
    }

    @Test
    @DisplayName("Tiempo de evento: un eventTime futuro va a la DLQ y no adelanta el stream time")
    void futuroNoAdelantaStreamTime() {
        enviar("e1", "T01", 10, 1, 1000);
        Instant futuro = T0.plusSeconds(7200);
        raw.pipeInput("T01", venta("fut", "T01", futuro, 1, 1000), T0.plusSeconds(11));
        enviar("e2", "T01", 20, 1, 1000); // si el futuro hubiera adelantado el stream time, esto seria tardio
        avanzarA(91);

        assertEquals(TipoError.REGLA_NEGOCIO, Json.leer(dlq.readValue(), RegistroDlq.class).errorType());
        assertTrue(descartadas.isEmpty());
        assertEquals(2, agregadosDe("T01").get(0).cantidadVentas());
    }
}
