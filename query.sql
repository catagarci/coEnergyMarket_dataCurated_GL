SELECT Tipo_transaccion, Nombre, Cantidad_comprada, Precio, Tipo_energia
FROM "co_energy_marketer_curated_pdn_rl"."energy_transactions"
WHERE tipo_energia = 'hidroelectrica'
  AND tipo_transaccion = 'venta'
  AND precio > 1000