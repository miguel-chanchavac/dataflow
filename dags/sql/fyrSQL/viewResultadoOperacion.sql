do $$
begin
	if not exists (select 1 from information_schema.views where table_name = 'resultado_operacion') then
		create view stage.resultado_operacion as
		select
			posicion ,
			cuenta_resumen ,
			'ingresos' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('4')
		group by posicion ,
		cuenta_resumen
		,to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		union all
		--costos de produccion
		select
			posicion ,
			cuenta_resumen ,
			'costo_produccion' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('5')
		group by posicion ,
		cuenta_resumen
		,to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		union all
		--otros ingresos
		select
			posicion ,
			cuenta_resumen ,
			'otros ingresos' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('13')
		group by posicion ,
		cuenta_resumen
		,to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		union all
		--gastos de operacion
		select
			posicion ,
			cuenta_resumen ,
			'gastos de operacion' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		and posicion in ('6.1','6.2','10','9')
		group by posicion ,
		cuenta_resumen
		,to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd');
	end if;
end $$