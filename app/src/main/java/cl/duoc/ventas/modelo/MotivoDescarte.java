package cl.duoc.ventas.modelo;

/** Ventas correctas que no se suman al agregado (van a ventas-descartadas, no a la DLQ). */
public enum MotivoDescarte {
    /** Mismo eventId ya aceptado dentro de la retencion de deduplicacion. */
    DUPLICADO,
    /** Su ventana ya cerro: fin de ventana + gracia <= stream time. */
    TARDIO_FUERA_DE_GRACIA
}
