package cl.duoc.ventas.streams;

import cl.duoc.ventas.modelo.RegistroDlq;
import cl.duoc.ventas.modelo.Venta;
import cl.duoc.ventas.modelo.VentaDescartada;

/** Resultado interno de cada etapa de la topologia (solo vive en memoria, no se serializa). */
public record Clasificacion(Venta venta, RegistroDlq dlq, VentaDescartada descarte) {

    static Clasificacion aceptada(Venta v) {
        return new Clasificacion(v, null, null);
    }

    static Clasificacion aDlq(RegistroDlq r) {
        return new Clasificacion(null, r, null);
    }

    static Clasificacion descartada(VentaDescartada d) {
        return new Clasificacion(d.venta(), null, d);
    }

    boolean esDlq() {
        return dlq != null;
    }

    boolean esDescarte() {
        return descarte != null;
    }
}
