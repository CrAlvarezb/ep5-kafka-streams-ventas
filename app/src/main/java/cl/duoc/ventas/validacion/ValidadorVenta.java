package cl.duoc.ventas.validacion;

import cl.duoc.ventas.Json;
import cl.duoc.ventas.modelo.TipoError;
import cl.duoc.ventas.modelo.Venta;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Valida una venta en tres niveles, en orden:
 * <ol>
 *   <li>Sintaxis: el valor es JSON y es un objeto -> si no, JSON_MALFORMADO.</li>
 *   <li>Campos: obligatorios presentes, con tipo y formato correcto (eventTime ISO-8601, enteros
 *       positivos) -> si no, CAMPO_INVALIDO.</li>
 *   <li>Negocio: total = cantidad x precioUnitario, eventTime no futuro, key = tiendaId -> si no,
 *       REGLA_NEGOCIO.</li>
 * </ol>
 * Un evento antiguo NO es invalido: si es tardio lo decide la ventana (stream time), no el validador.
 */
public final class ValidadorVenta {

    private final Duration futuroMaximo;

    public ValidadorVenta(Duration futuroMaximo) {
        this.futuroMaximo = futuroMaximo;
    }

    /**
     * @param key   key Kafka del registro
     * @param valor valor crudo (String)
     * @param ahora instante de referencia para la regla de fecha futura
     */
    public Resultado validar(String key, String valor, Instant ahora) {
        if (valor == null || valor.isBlank()) {
            return Resultado.error(TipoError.JSON_MALFORMADO, "valor vacio o nulo", null);
        }
        JsonNode n;
        try {
            n = Json.MAPPER.readTree(valor);
        } catch (JsonProcessingException e) {
            return Resultado.error(TipoError.JSON_MALFORMADO, "JSON no parseable: " + primeraLinea(e.getOriginalMessage()), null);
        }
        if (n == null || !n.isObject()) {
            return Resultado.error(TipoError.JSON_MALFORMADO, "el valor no es un objeto JSON", null);
        }

        String eventId = textoOpcional(n, "eventId");

        // --- Nivel 2: campos obligatorios y formato
        if (eventId == null) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, "eventId ausente o vacio", null);
        }
        String tiendaId = textoOpcional(n, "tiendaId");
        if (tiendaId == null) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, "tiendaId ausente o vacio", eventId);
        }
        JsonNode et = n.get("eventTime");
        if (et == null || et.isNull() || !et.isTextual() || et.asText().isBlank()) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, "eventTime ausente o no es texto", eventId);
        }
        Instant eventTime;
        try {
            eventTime = OffsetDateTime.parse(et.asText()).toInstant();
        } catch (DateTimeParseException e) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, "eventTime no es ISO-8601 con zona: '" + et.asText() + "'", eventId);
        }
        String errCantidad = enteroPositivo(n, "cantidad", true);
        if (errCantidad != null) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, errCantidad, eventId);
        }
        String errPrecio = enteroPositivo(n, "precioUnitario", false);
        if (errPrecio != null) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, errPrecio, eventId);
        }
        String errTotal = enteroPositivo(n, "total", false);
        if (errTotal != null) {
            return Resultado.error(TipoError.CAMPO_INVALIDO, errTotal, eventId);
        }
        int cantidad = n.get("cantidad").intValue();
        long precio = n.get("precioUnitario").longValue();
        long total = n.get("total").longValue();

        // --- Nivel 3: reglas de negocio
        long esperado;
        try {
            esperado = Math.multiplyExact(cantidad, precio);
        } catch (ArithmeticException e) {
            return Resultado.error(TipoError.REGLA_NEGOCIO, "cantidad x precioUnitario desborda", eventId);
        }
        if (esperado != total) {
            return Resultado.error(TipoError.REGLA_NEGOCIO,
                    "total " + total + " != cantidad " + cantidad + " x precioUnitario " + precio + " (= " + esperado + ")", eventId);
        }
        if (eventTime.isAfter(ahora.plus(futuroMaximo))) {
            // Un eventTime futuro adelantaria el stream time y cerraria ventanas antes de tiempo.
            return Resultado.error(TipoError.REGLA_NEGOCIO,
                    "eventTime " + eventTime + " esta mas de " + futuroMaximo.toSeconds() + " s en el futuro", eventId);
        }
        if (key == null || !key.equals(tiendaId)) {
            return Resultado.error(TipoError.REGLA_NEGOCIO,
                    "la key Kafka (" + key + ") no coincide con tiendaId (" + tiendaId + ")", eventId);
        }

        Venta v = new Venta(eventId, tiendaId, textoOpcional(n, "tiendaNombre"), eventTime,
                textoOpcional(n, "clienteId"), textoOpcional(n, "producto"), textoOpcional(n, "categoria"),
                textoOpcional(n, "canal"), textoOpcional(n, "medioPago"), cantidad, precio, total);
        return Resultado.ok(v);
    }

    private static String textoOpcional(JsonNode n, String campo) {
        JsonNode v = n.get(campo);
        if (v == null || v.isNull() || !v.isTextual() || v.asText().isBlank()) {
            return null;
        }
        return v.asText();
    }

    private static String enteroPositivo(JsonNode n, String campo, boolean int32) {
        JsonNode v = n.get(campo);
        if (v == null || v.isNull()) {
            return campo + " ausente";
        }
        if (!v.isIntegralNumber()) {
            return campo + " debe ser un numero entero (recibido: " + v + ")";
        }
        if (int32 ? !v.canConvertToInt() : !v.canConvertToLong()) {
            return campo + " fuera de rango";
        }
        if (v.longValue() <= 0) {
            return campo + " debe ser > 0 (recibido: " + v + ")";
        }
        return null;
    }

    private static String primeraLinea(String s) {
        if (s == null) {
            return "";
        }
        int i = s.indexOf('\n');
        return i < 0 ? s : s.substring(0, i);
    }

    /** Resultado de validar: o una venta tipada, o un error con su tipo. */
    public record Resultado(Venta venta, TipoError tipoError, String motivo, String eventId) {

        public static Resultado ok(Venta v) {
            return new Resultado(v, null, null, v.eventId());
        }

        public static Resultado error(TipoError tipo, String motivo, String eventId) {
            return new Resultado(null, tipo, motivo, eventId);
        }

        public boolean valido() {
            return venta != null;
        }
    }
}
