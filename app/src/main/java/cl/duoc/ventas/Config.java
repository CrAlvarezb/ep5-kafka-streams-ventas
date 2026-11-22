package cl.duoc.ventas;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Configuracion centralizada. Todo lo que cambia entre entornos (local, AWS) llega por variables de
 * entorno (ver app.env.example). Se valida al iniciar: si algo es invalido la app no arranca y dice
 * que variable revisar.
 */
public record Config(
        String bootstrapServers,
        String applicationId,
        String topicRaw,
        String topicAgregadas,
        String topicDlq,
        String topicDescartadas,
        Duration ventana,
        Duration gracia,
        Duration retencionDedup,
        Duration futuroMaximo,
        int replicationFactor,
        int streamThreads,
        String stateDir,
        double ventasPorSegundo) {

    public static Config desdeEntorno() {
        return desde(System.getenv());
    }

    public static Config desde(Map<String, String> env) {
        List<String> errores = new ArrayList<>();
        Config c = new Config(
                texto(env, "KAFKA_BOOTSTRAP_SERVERS", null, errores),
                texto(env, "APPLICATION_ID", "ventas-aggregator", errores),
                texto(env, "TOPIC_RAW", "ventas-raw", errores),
                texto(env, "TOPIC_AGREGADAS", "ventas-agregadas", errores),
                texto(env, "TOPIC_DLQ", "ventas-raw-dlq", errores),
                texto(env, "TOPIC_DESCARTADAS", "ventas-descartadas", errores),
                Duration.ofSeconds(entero(env, "VENTANA_SEGUNDOS", 60, 1, errores)),
                Duration.ofSeconds(entero(env, "GRACIA_SEGUNDOS", 30, 0, errores)),
                Duration.ofSeconds(entero(env, "DEDUP_RETENCION_SEGUNDOS", 600, 1, errores)),
                Duration.ofSeconds(entero(env, "FUTURO_MAX_SEGUNDOS", 300, 0, errores)),
                entero(env, "REPLICATION_FACTOR", 2, 1, errores),
                entero(env, "NUM_STREAM_THREADS", 1, 1, errores),
                texto(env, "STATE_DIR", "/tmp/kafka-streams", errores),
                decimal(env, "VENTAS_POR_SEGUNDO", 2.0, errores));
        c.validarCoherencia(errores);
        if (!errores.isEmpty()) {
            throw new IllegalArgumentException("Configuracion invalida: " + String.join("; ", errores));
        }
        return c;
    }

    private void validarCoherencia(List<String> errores) {
        if (ventana == null || gracia == null || retencionDedup == null) {
            return;
        }
        // Un duplicado que llega despues de ventana+gracia ya seria tardio y se descartaria igual;
        // con menos retencion podria colarse un duplicado dentro de una ventana aun abierta.
        if (retencionDedup.compareTo(ventana.plus(gracia)) < 0) {
            errores.add("DEDUP_RETENCION_SEGUNDOS debe ser >= VENTANA_SEGUNDOS + GRACIA_SEGUNDOS");
        }
        Set<String> topics = new HashSet<>();
        for (String t : List.of(topicRaw, topicAgregadas, topicDlq, topicDescartadas)) {
            if (t != null && !topics.add(t)) {
                errores.add("los topics deben ser distintos (repetido: " + t + ")");
            }
        }
        if (replicationFactor > 2) {
            errores.add("REPLICATION_FACTOR no puede ser > 2: el cluster tiene 2 brokers");
        }
    }

    public String resumen() {
        return String.format(
                "ventana=%d s, gracia=%d s, retencion dedup=%d s, futuro max=%d s, topics raw=%s agregados=%s dlq=%s descartes=%s, RF=%d, threads=%d",
                ventana.toSeconds(), gracia.toSeconds(), retencionDedup.toSeconds(), futuroMaximo.toSeconds(),
                topicRaw, topicAgregadas, topicDlq, topicDescartadas, replicationFactor, streamThreads);
    }

    private static String texto(Map<String, String> env, String nombre, String defecto, List<String> errores) {
        String v = env.get(nombre);
        if (v == null || v.isBlank()) {
            if (defecto == null) {
                errores.add(nombre + " es obligatoria");
            }
            return defecto;
        }
        return v.trim();
    }

    private static int entero(Map<String, String> env, String nombre, int defecto, int minimo, List<String> errores) {
        String v = env.get(nombre);
        if (v == null || v.isBlank()) {
            return defecto;
        }
        try {
            int n = Integer.parseInt(v.trim());
            if (n < minimo) {
                errores.add(nombre + " debe ser >= " + minimo + " (valor: " + v + ")");
            }
            return n;
        } catch (NumberFormatException e) {
            errores.add(nombre + " debe ser un entero (valor: " + v + ")");
            return defecto;
        }
    }

    private static double decimal(Map<String, String> env, String nombre, double defecto, List<String> errores) {
        String v = env.get(nombre);
        if (v == null || v.isBlank()) {
            return defecto;
        }
        try {
            double d = Double.parseDouble(v.trim());
            if (d <= 0) {
                errores.add(nombre + " debe ser > 0");
            }
            return d;
        } catch (NumberFormatException e) {
            errores.add(nombre + " debe ser numerico (valor: " + v + ")");
            return defecto;
        }
    }
}
