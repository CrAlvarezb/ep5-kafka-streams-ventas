package cl.duoc.ventas.productor;

import cl.duoc.ventas.modelo.Venta;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Datos ficticios para generar ventas realistas. */
public final class Catalogo {

    public record Tienda(String id, String nombre, String canal) {
    }

    public record Producto(String nombre, String categoria, long precioMin, long precioMax) {
    }

    /** Tiendas del flujo normal. T-DEMO queda reservada para los casos controlados. */
    public static final List<Tienda> TIENDAS = List.of(
            new Tienda("T01", "Santiago Centro", "presencial"),
            new Tienda("T02", "Providencia", "presencial"),
            new Tienda("T03", "Las Condes", "presencial"),
            new Tienda("T04", "Maipu", "presencial"),
            new Tienda("T05", "Vina del Mar", "presencial"),
            new Tienda("T06", "Concepcion", "presencial"),
            new Tienda("T07", "Tienda Online", "web"),
            new Tienda("T08", "App Movil", "app"));

    public static final Tienda DEMO = new Tienda("T-DEMO", "Tienda Demo", "presencial");

    private static final List<Producto> PRODUCTOS = List.of(
            new Producto("Leche 1 L", "supermercado", 990, 1490),
            new Producto("Arroz 1 kg", "supermercado", 1090, 1690),
            new Producto("Cafe 250 g", "supermercado", 3490, 6990),
            new Producto("Polera algodon", "vestuario", 7990, 14990),
            new Producto("Chaqueta cortaviento", "vestuario", 29990, 59990),
            new Producto("Audifonos", "tecnologia", 12990, 49990),
            new Producto("Cargador USB-C", "tecnologia", 6990, 15990),
            new Producto("Set de sartenes", "hogar", 19990, 39990));

    private static final List<String> MEDIOS = List.of("debito", "credito", "efectivo", "transferencia");

    private Catalogo() {
    }

    public static Venta ventaAleatoria(Instant eventTime) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        Tienda t = TIENDAS.get(r.nextInt(TIENDAS.size()));
        Producto p = PRODUCTOS.get(r.nextInt(PRODUCTOS.size()));
        int cantidad = 1 + r.nextInt(4);
        long precio = (r.nextLong(p.precioMin(), p.precioMax() + 1) / 10) * 10;
        return new Venta(UUID.randomUUID().toString(), t.id(), t.nombre(), eventTime,
                String.format("cliente-%04d", 1 + r.nextInt(200)), p.nombre(), p.categoria(), t.canal(),
                MEDIOS.get(r.nextInt(MEDIOS.size())), cantidad, precio, cantidad * precio);
    }

    public static Venta ventaDemo(String eventId, Instant eventTime, int cantidad, long precio) {
        return new Venta(eventId, DEMO.id(), DEMO.nombre(), eventTime, "cliente-demo", "Producto demo", "demo",
                DEMO.canal(), "debito", cantidad, precio, cantidad * precio);
    }
}
