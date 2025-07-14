package UT2.Actividades.Integradora.Oracle;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class ActIntegradoraO {
    public static void main(String[] args) {
        try {
            // Conexión a la base de datos
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conexion = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "system", "contaed");

            // Elegir el esquema
            try (Statement stmt = conexion.createStatement()) {
                stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = integradora");
            }

            Statement sentence = conexion.createStatement();

            // Borrar datos existentes en las tablas
            borrarDatos(conexion);
            System.out.println("--------------------------------------------------------------");

            // Leer e insertar los registros
            insertarRegistros(conexion);
            System.out.println("--------------------------------------------------------------");

            // Ejecutar las consultas
            leerConsultas(sentence, "Actividades/Integradora/sentencias.sql");

            // Ejecutar el procedimiento almacenado
            llamarProcedimiento(conexion);
            mostrarTablaProveedores(sentence);

            // Actualizar el stock y ver resultado del trigger
            actualizarStockTrigger(conexion);
            mostrarTablaPedidosPendientes(sentence);

        } catch (IOException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void borrarDatos(Connection conexion) throws SQLException {
        try (Statement stmt = conexion.createStatement()) {
            stmt.executeUpdate("DELETE FROM productos");
            stmt.executeUpdate("DELETE FROM proveedores");
            System.out.println("Registros eliminados de las tablas Productos y Proveedores.");
        }
    }

    private static void insertarRegistros(Connection conexion) throws IOException, SQLException {
        try (BufferedReader br = new BufferedReader(new FileReader("Actividades/Integradora/registros.sql"))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                String[] values = line.split(", ");

                if (values[0].startsWith("prv")) {
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

    private static void leerConsultas(Statement sentence, String rutaarchivo) throws IOException, SQLException {
        try (BufferedReader br = new BufferedReader(new FileReader(rutaarchivo))) {
            StringBuilder query = new StringBuilder();
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }
                query.append(line).append(" ");

                if (line.endsWith(";")) {
                    String queryleido = query.toString().replace(";", "").trim();
                    ejecutarConsulta(sentence, queryleido);
                    query.setLength(0);
                }
            }
        }
    }

    private static void ejecutarConsulta(Statement sentence, String queryleido) throws SQLException {
        if (queryleido.toLowerCase().startsWith("select")) {
            ejecutarSentencia(sentence, queryleido);
        } else {
            ejecutarActualizacion(sentence, queryleido);
        }
    }

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

    private static void ejecutarActualizacion(Statement sentence, String queryleido) throws SQLException {
        int colmodificadas = sentence.executeUpdate(queryleido);
        System.out.println("Registros actualizados: " + colmodificadas);
        System.out.println("--------------------------------------------------------------");
        mostrarTablaProductos(sentence);
    }

    private static void mostrarTablaProductos(Statement sentence) throws SQLException {
        System.out.println("Tabla productos tras la actualización de precios:");
        String qproductos = "SELECT * FROM productos";
        ejecutarSentencia(sentence, qproductos);
    }

    private static void mostrarTablaProveedores(Statement sentence) throws SQLException {
        System.out.println("Tabla proveedores después de actualizar bonificación:");
        String qproveedores = "SELECT * FROM proveedores";
        ejecutarSentencia(sentence, qproveedores);
    }

    private static void llamarProcedimiento(Connection conexion) throws SQLException {
        String sql = "{call integradora.proc_alm(?, ?)}";
        CallableStatement llamada = conexion.prepareCall(sql);

        llamada.setString(1, "prv2");
        llamada.setInt(2, 7526);

        llamada.executeUpdate();
        System.out.println("Procedimiento ejecutado y bonificación actualizada.");

        llamada.close();
    }

    private static void actualizarStockTrigger(Connection conexion) throws SQLException {
        String actstock = "UPDATE productos SET stock = 0 WHERE cod_prod = 'prd1'";
        try (PreparedStatement pstmt = conexion.prepareStatement(actstock)) {
            pstmt.executeUpdate();
            System.out.println("Se ha actualizado el stock ");
        }
    }

    private static void mostrarTablaPedidosPendientes(Statement sentence) throws SQLException {
        System.out.println("Tabla pedidos_pendientes tras la ejecución del trigger:");
        String qpedidospendientes = "SELECT * FROM pedidos_pendientes";
        ejecutarSentencia(sentence, qpedidospendientes);
    }
}
