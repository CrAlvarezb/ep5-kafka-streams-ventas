package cl.duoc.ventas;

import cl.duoc.ventas.productor.CasosApp;
import cl.duoc.ventas.productor.ConciliarApp;
import cl.duoc.ventas.productor.ProducerApp;
import cl.duoc.ventas.streams.StreamsApp;
import java.util.Arrays;

/** Punto de entrada unico: java -jar ventas-app.jar producer|streams|casos|conciliar [...] */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String modo = args.length > 0 ? args[0] : "ayuda";
        String[] resto = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0];
        if (modo.equals("ayuda") || modo.equals("--help")) {
            System.out.println("uso: ventas-app producer | streams | casos <caso|todos|lista> | conciliar [n_ventanas]");
            return;
        }
        Config cfg;
        try {
            cfg = Config.desdeEntorno();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        switch (modo) {
            case "producer" -> ProducerApp.ejecutar(cfg);
            case "streams" -> StreamsApp.ejecutar(cfg);
            case "casos" -> CasosApp.ejecutar(cfg, resto);
            case "conciliar" -> ConciliarApp.ejecutar(cfg, resto);
            default -> {
                System.err.println("modo desconocido: " + modo);
                System.exit(2);
            }
        }
    }
}
