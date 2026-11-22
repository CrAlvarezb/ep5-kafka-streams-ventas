package cl.duoc.ventas.productor;

import cl.duoc.ventas.Config;
import cl.duoc.ventas.Json;
import cl.duoc.ventas.modelo.Venta;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Generador de casos controlados y reproducibles sobre la tienda T-DEMO.
 * Uso: casos &lt;caso&gt; | casos todos | casos lista | casos reinicio &lt;eventId&gt;
 */
public final class CasosApp {

    private final Config cfg;
    private final KafkaProducer<String, String> productor;
    private final String tag = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[-:TZ]", "").substring(8);

    private CasosApp(Config cfg, KafkaProducer<String, String> productor) {
        this.cfg = cfg;
        this.productor = productor;
    }

    private static final Map<String, String> CASOS = new LinkedHashMap<>();

    static {
        CASOS.put("valida", "venta correcta -> se suma al agregado de T-DEMO");
        CASOS.put("json-malformado", "JSON truncado -> DLQ JSON_MALFORMADO");
        CASOS.put("campo-faltante", "sin eventTime -> DLQ CAMPO_INVALIDO");
        CASOS.put("campo-invalido", "cantidad como texto -> DLQ CAMPO_INVALIDO");
        CASOS.put("total-incorrecto", "total != cantidad x precio -> DLQ REGLA_NEGOCIO");
        CASOS.put("fecha-futura", "eventTime 2 h en el futuro -> DLQ REGLA_NEGOCIO");
        CASOS.put("duplicado", "misma venta (mismo eventId) 2 veces -> 1.a suma, 2.a ventas-descartadas DUPLICADO");
        CASOS.put("tardio-en-gracia", "eventTime 15 s anterior al mas reciente -> se suma a su ventana original");
        CASOS.put("tardio-fuera-de-gracia", "eventTime 3 min anterior -> ventas-descartadas TARDIO_FUERA_DE_GRACIA (no DLQ)");
    }

    public static void ejecutar(Config cfg, String[] args) throws Exception {
        String caso = args.length > 0 ? args[0] : "lista";
        if (caso.equals("lista") || caso.equals("ayuda")) {
            System.out.println("Casos disponibles (tienda T-DEMO):");
            CASOS.forEach((k, v) -> System.out.printf("  %-24s %s%n", k, v));
            System.out.println("  todos                    ejecuta todos en orden");
            System.out.println("  reinicio <eventId>       envia una venta fija con ese eventId (prueba de reinicio)");
            return;
        }
        try (KafkaProducer<String, String> p = new KafkaProducer<>(ProducerApp.propiedades(cfg, "ventas-casos"))) {
            CasosApp app = new CasosApp(cfg, p);
            switch (caso) {
                case "todos" -> {
                    for (String c : CASOS.keySet()) {
                        app.correr(c);
                        Thread.sleep(300);
                    }
                    System.out.printf("%nLos agregados de T-DEMO aparecen en %s al cerrar cada ventana (fin + %d s de gracia).%n",
                            cfg.topicAgregadas(), cfg.gracia().toSeconds());
                }
                case "reinicio" -> {
                    if (args.length < 2) {
                        throw new IllegalArgumentException("uso: casos reinicio <eventId>");
                    }
                    Venta v = Catalogo.ventaDemo(args[1], Instant.now(), 1, 4000);
                    app.enviar("reinicio", v.tiendaId(), Json.escribir(v), "1.a vez: se suma; 2.a vez: DUPLICADO (aunque Streams se reinicie)");
                }
                default -> {
                    if (!CASOS.containsKey(caso)) {
                        throw new IllegalArgumentException("caso desconocido: " + caso + " (usa: casos lista)");
                    }
                    app.correr(caso);
                }
            }
        }
    }

    private void correr(String caso) throws Exception {
        Instant ahora = Instant.now();
        String key = Catalogo.DEMO.id();
        System.out.printf("%n== %s: %s%n", caso, CASOS.get(caso));
        switch (caso) {
            case "valida" -> enviar(caso, key, json(Catalogo.ventaDemo(id("valida"), ahora, 1, 5000)), "suma 1 venta / $5.000");
            case "json-malformado" -> enviar(caso, key,
                    "{\"eventId\":\"" + id("malformado") + "\",\"tiendaId\":\"T-DEMO\",\"eventTime\":\"" + ahora + "\",\"cantidad\":1,",
                    "DLQ JSON_MALFORMADO, rawValue intacto");
            case "campo-faltante" -> enviar(caso, key,
                    "{\"eventId\":\"" + id("sin-eventtime") + "\",\"tiendaId\":\"T-DEMO\",\"cantidad\":1,\"precioUnitario\":5000,\"total\":5000}",
                    "DLQ CAMPO_INVALIDO (eventTime ausente)");
            case "campo-invalido" -> enviar(caso, key,
                    "{\"eventId\":\"" + id("cantidad-texto") + "\",\"tiendaId\":\"T-DEMO\",\"eventTime\":\"" + ahora
                            + "\",\"cantidad\":\"dos\",\"precioUnitario\":5000,\"total\":10000}",
                    "DLQ CAMPO_INVALIDO (cantidad no es entero)");
            case "total-incorrecto" -> {
                Venta v = Catalogo.ventaDemo(id("total-malo"), ahora, 2, 5000);
                Venta mala = new Venta(v.eventId(), v.tiendaId(), v.tiendaNombre(), v.eventTime(), v.clienteId(), v.producto(),
                        v.categoria(), v.canal(), v.medioPago(), 2, 5000, 9999);
                enviar(caso, key, json(mala), "DLQ REGLA_NEGOCIO (2 x 5000 != 9999)");
            }
            case "fecha-futura" -> enviar(caso, key, json(Catalogo.ventaDemo(id("futura"), ahora.plus(Duration.ofHours(2)), 1, 5000)),
                    "DLQ REGLA_NEGOCIO (no adelanta el stream time)");
            case "duplicado" -> {
                String json = json(Catalogo.ventaDemo(id("duplicado"), ahora, 2, 5000));
                enviar(caso + " (1.a)", key, json, "se suma: 1 venta / $10.000");
                enviar(caso + " (2.a)", key, json, "ventas-descartadas DUPLICADO; NO suma");
            }
            case "tardio-en-gracia" -> {
                enviar("ancla", key, json(Catalogo.ventaDemo(id("ancla-g"), ahora, 1, 1000)), "fija el stream time en " + ahora);
                Instant t = ahora.minusSeconds(15);
                enviar(caso, key, json(Catalogo.ventaDemo(id("tardio-gracia"), t, 1, 7000)),
                        "eventTime " + t + ": su ventana sigue abierta -> se suma a su ventana original ($7.000)");
            }
            case "tardio-fuera-de-gracia" -> {
                enviar("ancla", key, json(Catalogo.ventaDemo(id("ancla-f"), ahora, 1, 1000)), "fija el stream time en " + ahora);
                Instant t = ahora.minusSeconds(180);
                enviar(caso, key, json(Catalogo.ventaDemo(id("tardio-fuera"), t, 1, 9000)),
                        "eventTime " + t + ": ventana cerrada -> ventas-descartadas TARDIO_FUERA_DE_GRACIA, no DLQ");
            }
            default -> throw new IllegalStateException(caso);
        }
    }

    private String id(String prefijo) {
        return "demo-" + prefijo + "-" + tag + "-" + UUID.randomUUID().toString().substring(0, 4);
    }

    private static String json(Venta v) {
        return Json.escribir(v);
    }

    private void enviar(String caso, String key, String valor, String esperado) throws Exception {
        RecordMetadata md = productor.send(new ProducerRecord<>(cfg.topicRaw(), key, valor)).get();
        System.out.printf("  %-28s -> %s-%d@%d key=%s | esperado: %s%n", caso, md.topic(), md.partition(), md.offset(), key, esperado);
        System.out.printf("  %-28s    valor: %s%n", "", valor.length() > 220 ? valor.substring(0, 220) + "..." : valor);
    }
}
