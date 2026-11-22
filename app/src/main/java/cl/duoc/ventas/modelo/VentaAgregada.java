package cl.duoc.ventas.modelo;

import java.time.Instant;

/**
 * Resultado de ventas-agregadas: un registro por tienda y ventana, publicado al cerrar la ventana.
 * La ventana es [windowStart, windowEnd).
 */
public record VentaAgregada(
        String tiendaId,
        Instant windowStart,
        Instant windowEnd,
        long cantidadVentas,
        long unidades,
        long montoTotal,
        long ticketPromedio,
        Instant primerEvento,
        Instant ultimoEvento,
        Instant publicadoEn) {

    public static VentaAgregada de(String tiendaId, Instant inicio, Instant fin, Acumulado a, Instant ahora) {
        long ticket = a.cantidadVentas() == 0 ? 0 : Math.round((double) a.montoTotal() / a.cantidadVentas());
        return new VentaAgregada(tiendaId, inicio, fin, a.cantidadVentas(), a.unidades(), a.montoTotal(), ticket,
                a.primerEvento(), a.ultimoEvento(), ahora);
    }
}
