package cl.duoc.ventas.productor;

import cl.duoc.ventas.Config;
import cl.duoc.ventas.Json;
import cl.duoc.ventas.modelo.Venta;
import cl.duoc.ventas.modelo.VentaAgregada;
import cl.duoc.ventas.modelo.VentaDescartada;
import cl.duoc.ventas.validacion.ValidadorVenta;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Conciliacion "sin perdida silenciosa": para cada ventana cerrada compara
 * <ul>
 *   <li>A = ventas validas de ventas-raw, excluyendo los registros auditados en ventas-descartadas
 *       (duplicados y tardios fuera de gracia, identificados por particion@offset de origen), y</li>
 *   <li>B = suma de cantidadVentas publicada en ventas-agregadas.</li>
 * </ul>
 * Diferencia 0 en todas las ventanas = nada se perdio ni se conto dos veces. Lee los topics con
 * assign() (sin consumer group): no altera offsets de nadie.
 */
public final class ConciliarApp {

    private ConciliarApp() {
    }

    public static void ejecutar(Config cfg, String[] args) {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 20;
        ValidadorVenta validador = new ValidadorVenta(cfg.futuroMaximo());
        long ventanaMs = cfg.ventana().toMillis();

        Set<String> descartados = new HashSet<>();
        leer(cfg, cfg.topicDescartadas(), r -> {
            VentaDescartada d = Json.leer(r.value(), VentaDescartada.class);
            descartados.add(d.sourcePartition() + "@" + d.sourceOffset());
        });

        Map<String, long[]> esperado = new TreeMap<>(); // ventana|tienda -> {ventas, monto}
        long[] maxEventTime = {0};
        long[] contadores = new long[3]; // raw total, invalidos, descartados
        leer(cfg, cfg.topicRaw(), r -> {
            contadores[0]++;
            // la regla de fecha futura se evalua contra el momento en que se produjo el registro
            ValidadorVenta.Resultado res = validador.validar(r.key(), r.value(), Instant.ofEpochMilli(r.timestamp()));
            if (!res.valido()) {
                contadores[1]++;
                return;
            }
            Venta v = res.venta();
            maxEventTime[0] = Math.max(maxEventTime[0], v.eventTime().toEpochMilli());
            if (descartados.contains(r.partition() + "@" + r.offset())) {
                contadores[2]++;
                return;
            }
            long inicio = Math.floorDiv(v.eventTime().toEpochMilli(), ventanaMs) * ventanaMs;
            long[] acc = esperado.computeIfAbsent(Instant.ofEpochMilli(inicio) + "|" + v.tiendaId(), k -> new long[2]);
            acc[0]++;
            acc[1] += v.total();
        });

        Map<String, long[]> publicado = new TreeMap<>();
        leer(cfg, cfg.topicAgregadas(), r -> {
            VentaAgregada a = Json.leer(r.value(), VentaAgregada.class);
            long[] acc = publicado.computeIfAbsent(a.windowStart() + "|" + a.tiendaId(), k -> new long[2]);
            acc[0] += a.cantidadVentas();
            acc[1] += a.montoTotal();
        });

        // Solo ventanas ya cerradas: fin + gracia <= mayor eventTime visto (con 1 ventana de margen).
        long limite = maxEventTime[0] - cfg.gracia().toMillis() - ventanaMs;
        TreeMap<String, long[][]> porVentana = new TreeMap<>();
        Set<String> claves = new HashSet<>(esperado.keySet());
        claves.addAll(publicado.keySet());
        for (String k : claves) {
            String ventana = k.substring(0, k.indexOf('|'));
            if (Instant.parse(ventana).toEpochMilli() + ventanaMs > limite) {
                continue;
            }
            long[][] fila = porVentana.computeIfAbsent(ventana, x -> new long[][] {{0, 0}, {0, 0}});
            long[] e = esperado.getOrDefault(k, new long[2]);
            long[] p = publicado.getOrDefault(k, new long[2]);
            fila[0][0] += e[0];
            fila[0][1] += e[1];
            fila[1][0] += p[0];
            fila[1][1] += p[1];
        }
        List<String> ultimas = new ArrayList<>(porVentana.keySet());
        ultimas = ultimas.subList(Math.max(0, ultimas.size() - n), ultimas.size());

        System.out.printf("ventas-raw leidos=%d invalidos(DLQ)=%d descartados(dup/tardio)=%d%n", contadores[0], contadores[1], contadores[2]);
        System.out.printf("%-22s %12s %12s %8s %16s %16s %8s%n", "ventana (UTC)", "raw validas", "agregadas", "dif", "monto raw", "monto agregado", "dif");
        long difTotal = 0;
        for (String w : ultimas) {
            long[][] f = porVentana.get(w);
            long dv = f[0][0] - f[1][0];
            long dm = f[0][1] - f[1][1];
            difTotal += Math.abs(dv) + Math.abs(dm);
            System.out.printf("%-22s %12d %12d %8d %16d %16d %8d%n", w, f[0][0], f[1][0], dv, f[0][1], f[1][1], dm);
        }
        System.out.printf("%nVentanas comparadas: %d. %s%n", ultimas.size(),
                difTotal == 0 ? "RESULTADO: diferencia 0 en todas (sin perdidas ni dobles conteos)" : "RESULTADO: HAY DIFERENCIAS");
    }

    private static void leer(Config cfg, String topic, Consumer<ConsumerRecord<String, String>> accion) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic)) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> fin = c.endOffsets(tps);
            while (tps.stream().anyMatch(tp -> c.position(tp) < fin.get(tp))) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(2))) {
                    accion.accept(r);
                }
            }
        }
    }
}
