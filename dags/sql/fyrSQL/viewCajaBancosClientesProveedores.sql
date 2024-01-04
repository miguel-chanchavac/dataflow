do $$
begin
	if not exists (select 1 from information_schema.views WHERE table_name = 'cajabancos_cliente_proveedores') then
		create view stage.cajabancos_cliente_proveedores as
		with cliente as (
		select
			posicion ,
			cuenta_resumen  ,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial) as saldo_inicial,
			sum(debe_2) as debe,
			sum(haber_2) as haber
		from stage.stage_ingreso_historia sih
		where 1=1
		and cuenta_resumen in ('CLIENTES')
		and nombre_cuenta in ('CLIENTES LOCALES')
		group by posicion ,
		cuenta_resumen,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		order by fecha_operacion, cuenta_resumen
		), caja_bancos as (
		select
			posicion ,
			cuenta_resumen  ,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial) as saldo_inicial,
			sum(debe_2) as debe,
			sum(haber_2) as haber
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('1.8') --CAJA Y BANCOS
		group by posicion ,
		cuenta_resumen,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		order by fecha_operacion, cuenta_resumen
		), proveedor as (
		select
			posicion ,
			cuenta_resumen  ,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			--lag(fecha_operacion, 1) over (order by to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')) as fecha_anterior
			sum(saldo_inicial) as saldo_inicial,
			sum(debe_2) as debe,
			sum(haber_2) as haber
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('4.3') --PROVEEDORES
		group by posicion ,
		cuenta_resumen,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		order by fecha_operacion, cuenta_resumen
		)
		select *,
		case
			when fecha_operacion = date_trunc('YEAR', fecha_operacion+5 - interval '5 day')::date then lag(saldo_inicial, 1) over (order by fecha_operacion)+debe-haber
			else saldo_inicial+debe-haber
		end as saldo_final
		from cliente
		union all
		select *
		,case
			when fecha_operacion = date_trunc('YEAR', fecha_operacion+5 - interval '5 day')::date then lag(saldo_inicial, 1) over (order by cuenta_resumen, fecha_operacion)+debe-haber
			else saldo_inicial+debe-haber
		end as saldo_final
		from caja_bancos
		union all
		select *
		,case
			when fecha_operacion = date_trunc('YEAR', fecha_operacion+5 - interval '5 day')::date then lag(saldo_inicial, 1) over (order by cuenta_resumen, fecha_operacion)+debe-haber
			else saldo_inicial+debe-haber
		end as saldo_final
		from proveedor;
	end if;
end $$