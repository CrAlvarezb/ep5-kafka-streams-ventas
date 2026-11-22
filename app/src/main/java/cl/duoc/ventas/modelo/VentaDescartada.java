package cl.duoc.ventas.modelo;

import java.time.Instant;

/** Registro de auditoria en ventas-descartadas (duplicados y tardios fuera de gracia). */
public record VentaDescartada(
        MotivoDescarte motivo,
        String detalle,
        String eventId,
        String tiendaId,
        Instant eventTime,
        Instant windowStart,
        Instant windowEnd,
        Instant streamTime,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        Instant descartadaEn,
        Venta venta) {
}
