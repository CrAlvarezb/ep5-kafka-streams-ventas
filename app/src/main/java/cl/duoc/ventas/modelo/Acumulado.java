package cl.duoc.ventas.modelo;

import java.time.Instant;

/** Estado intermedio de la ventana (vive en el state store ventas-por-tienda). */
public record Acumulado(long cantidadVentas, long unidades, long montoTotal, Instant primerEvento, Instant ultimoEvento) {

    public static Acumulado vacio() {
        return new Acumulado(0, 0, 0, null, null);
    }

    public Acumulado sumar(Venta v) {
        Instant primero = primerEvento == null || v.eventTime().isBefore(primerEvento) ? v.eventTime() : primerEvento;
        Instant ultimo = ultimoEvento == null || v.eventTime().isAfter(ultimoEvento) ? v.eventTime() : ultimoEvento;
        return new Acumulado(cantidadVentas + 1, unidades + v.cantidad(), montoTotal + v.total(), primero, ultimo);
    }
}
