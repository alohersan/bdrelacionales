SELECT Nombre_prod, Nombre_prov FROM Productos JOIN Proveedores ON Productos.Cod_prov = Proveedores.Cod_prov WHERE Precio > 2000 ORDER BY Precio DESC;
SELECT Nombre_prov, Telefono FROM Proveedores JOIN Productos ON Proveedores.Cod_prov = Productos.Cod_prov WHERE Nombre_prod LIKE '%Ordenador%';
SELECT Nombre_prod FROM Productos WHERE Stock < 20;
UPDATE Productos SET Precio = Precio * 0.95 WHERE Cod_prov IN (SELECT Cod_prov FROM Proveedores WHERE Bonifica = 0);
SELECT Nombre_prov, COUNT(*) AS num_productos, AVG(Precio) AS precio_medio FROM Productos JOIN Proveedores ON Productos.Cod_prov = Proveedores.Cod_prov GROUP BY Nombre_prov;
SELECT Nombre_prov, Direccion, Telefono FROM Proveedores JOIN Productos ON Proveedores.Cod_prov = Productos.Cod_prov WHERE Stock = (SELECT MAX(Stock) FROM Productos);

