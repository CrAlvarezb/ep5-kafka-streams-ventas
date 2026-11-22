package cl.duoc.ventas.modelo;

/** Tipos de rechazo hacia la DLQ. */
public enum TipoError {
    /** El valor no es JSON o no es un objeto JSON. */
    JSON_MALFORMADO,
    /** Falta un campo obligatorio o su tipo/formato es invalido. */
    CAMPO_INVALIDO,
    /** El JSON es correcto pero viola una regla de negocio (total, fecha futura, key). */
    REGLA_NEGOCIO
}
