package UT2.Actividades.Integradora.PostgreSQL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class ActIntegradoraP {
    public static void main(String[] args) throws SQLException {
        try {
            // Conexión a la base de datos
            Class.forName("org.postgresql.Driver");
            Connection conexion = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Integradora", "postgres", "contaed");
            Statement sentence = conexion.createStatement();

            // Borrar datos existentes en las tablas
            borrarDatos(conexion);
            System.out.println("--------------------------------------------------------------");

            // Leer e insertar los registros
            insertarregistros(conexion);
            System.out.println("--------------------------------------------------------------");

            // Ejecutar las consultas
            leerconsultas(sentence, "Actividades/Integradora/sentencias.sql");

            // Ejecutar el procedimiento almacenado
            llamarprocedimiento(conexion);
            mostrarTablaProveedores(sentence);//mostrar la tabla tras la ejecucion del procedimiento para ver los cambios

            // Actualizar el stock y ver resultado del trigger
            triggerstock(conexion);
            mostrarTablaPedidosPendientes(sentence);//mostrar la tabla tras la ejecucion del procedimiento para ver los cambios

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    //Metodo para vaciar las tablas antes de comenzar
    private static void borrarDatos(Connection conexion) throws SQLException {
        try (Statement stmt = conexion.createStatement()) {
            stmt.executeUpdate("DELETE FROM Productos;");
            stmt.executeUpdate("DELETE FROM Proveedores;");
            System.out.println("Registros eliminados de las tablas Productos y Proveedores.");
        }
    }

    //Leer los registros de registros.sql e insertarlos
    private static void insertarregistros(Connection conexion) throws IOException, SQLException {
        try (BufferedReader br = new BufferedReader(new FileReader("Actividades/Integradora/registros.sql"))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                String[] values = line.split(", ");//dejar un espacio igual que en el .sql

                if (values[0].startsWith("prv")) {
                    //Insertar datos en la tabla proveedores
                    String qprov = "INSERT INTO proveedores (cod_prov, nombre_prov, direccion, telefono, bonifica) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conexion.prepareStatement(qprov)) {
                        ps.setString(1, values[0]);
                        ps.setString(2, values[1]);
                        ps.setString(3, values[2]);
                        ps.setString(4, values[3]);
                        ps.setInt(5, Integer.parseInt(values[4]));
                        ps.executeUpdate();
                        System.out.println("Insertado en Proveedores: " + line);
                    }
                } else if (values[0].startsWith("prd")) {
                    //Insertar datos en la tabla productos
                    String qprod = "INSERT INTO productos (cod_prod, nombre_prod, precio, stock, cod_prov) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conexion.prepareStatement(qprod)) {
                        ps.setString(1, values[0]);
                        ps.setString(2, values[1]);
                        ps.setDouble(3, Double.parseDouble(values[2]));
                        ps.setInt(4, Integer.parseInt(values[3]));
                        ps.setString(5, values[4]);
                        ps.executeUpdate();
                        System.out.println("Insertado en Productos: " + line);
                    }
                }
            }
        }
    }

    //Leer las consultas de sentencias.sql y ejecutarlas
    private static void leerconsultas(Statement sentence, String rutaarchivo) throws IOException, SQLException {
        try (BufferedReader br = new BufferedReader(new FileReader(rutaarchivo))) {
            StringBuilder query = new StringBuilder();
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    //saltar los comentarios
                    continue;
                }
                query.append(line).append(" ");

                if (line.endsWith(";")) {
                    String queryleido = query.toString().replace(";", "").trim();
                    ejecutarqleido(sentence, queryleido);
                    query.setLength(0);//vaciar la string para pasar a la siguiente sentencia
                }
            }
        }
    }

    //Ejecutar las sentencias dependiendo de si son seleccion o actualizacion
    private static void ejecutarqleido(Statement sentence, String queryleido) throws SQLException {
        if (queryleido.toLowerCase().startsWith("select")) {
            ejecutarSentencia(sentence, queryleido);
        } else {
            ejecutarActualizacion(sentence, queryleido);
        }
    }

    //Ejecutar las sentencias de seleccion
    private static void ejecutarSentencia(Statement sentence, String queryleido) throws SQLException {
        ResultSet resul = sentence.executeQuery(queryleido);
        ResultSetMetaData metadata = resul.getMetaData();
        int column = metadata.getColumnCount();

        for (int i = 1; i <= column; i++) {
            System.out.print(String.format("%-30s", metadata.getColumnName(i)));
        }
        System.out.println();

        while (resul.next()) {
            for (int i = 1; i <= column; i++) {
                System.out.print(String.format("%-30s", resul.getString(i)));
            }
            System.out.println();
        }
        System.out.println("--------------------------------------------------------------");
    }

    //Ejecutar los update/delete
    private static void ejecutarActualizacion(Statement sentence, String queryleido) throws SQLException {
        int colmodificadas = sentence.executeUpdate(queryleido);
        System.out.println("Registros actualizados: " + colmodificadas);
        System.out.println("--------------------------------------------------------------");
        mostrarTablaProductos(sentence);//mostrar la tabla para ver los cambios
    }

    //mostrar la tabla productos tras la ejecucion de la query 4
    private static void mostrarTablaProductos(Statement sentence) throws SQLException {
        System.out.println("Tabla productos tras la actualizacion de precios:");
        String qproductos = "SELECT * FROM productos";
        ejecutarSentencia(sentence, qproductos);
    }

    //mostrar la tabla proveedores tras el procedimiento
    private static void mostrarTablaProveedores(Statement sentence) throws SQLException {
        System.out.println("Tabla proveedores después de actualizar bonificación:");
        String qproveedores = "SELECT * FROM proveedores";
        ejecutarSentencia(sentence, qproveedores);
    }

    //Procedimiento almacenado que actualiza la bonificacion del proveedor indicado
    private static void llamarprocedimiento(Connection conexion) throws SQLException {
        // Llamar al procedimiento almacenado
        String sql = "call proc_alm(?, ?)";
        CallableStatement llamada = conexion.prepareCall(sql);

        // Establecer los parámetros para el procedimiento
        llamada.setString(1, "prv2");  //cod_prov
        llamada.setInt(2, 7526);  //nueva bonificacion

        // Ejecutar el procedimiento
        llamada.executeUpdate();
        System.out.println("Procedimiento ejecutado y bonificación actualizada.");

        llamada.close();
    }

    // Actualizar el stock de un producto para activar el trigger
    private static void triggerstock(Connection conexion) throws SQLException {
        String actstock = "UPDATE productos SET stock = ? WHERE cod_prod = ?";
        try (PreparedStatement pstmt = conexion.prepareStatement(actstock)) {
            pstmt.setInt(1, 0); // nuevo stock
            pstmt.setString(2, "prd5"); // cod_prod
            pstmt.executeUpdate();
            System.out.println("Se ha actualiza el stock.");
        }
    }
    private static void mostrarTablaPedidosPendientes(Statement sentence) throws SQLException {
        String qpedidospendientes = "SELECT * FROM pedidos_pendientes";
        ejecutarSentencia(sentence, qpedidospendientes);
    }
}
