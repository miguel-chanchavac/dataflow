do $$
begin
	if not exists (select 1 from information_schema.views WHERE table_name = 'liquidez_fyr') then
		create view stage.liquidez_fyr as
		---todos los activos
		with activo as (
		--activos corrientes
		select
			posicion ,
			cuenta_resumen ,
			'activos_corrientes' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber) as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		--and to_char(fecha_operacion, 'yyyy') = '2023'
		and posicion in ('1.8', '1.5', '1.4', '1.6', '1.7', '1.9')
		group by posicion ,
		cuenta_resumen ,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		union all
		--activos no corrientes
		select
			posicion ,
			cuenta_resumen ,
			'activos_no_corrientes' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber) as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		--and to_char(fecha_operacion, 'yyyy') = '2023'
		and posicion in ('1.1', '1.2', '1.2.1', '1.3')
		group by posicion ,
		cuenta_resumen ,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		)
		--capital de reserva
		, pasivo as (
		--pasivo no corriente
		select
			posicion ,
			cuenta_resumen ,
			'pasivo_no_corriente' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		--and to_char(fecha_operacion, 'yyyy') = '2023'
		and posicion in ('4.1','4.2')
		group by posicion ,
		cuenta_resumen ,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		--pasivo corriente
		union all
		select
			posicion ,
			cuenta_resumen ,
			'pasivo_corriente' as tipo_cuenta,
			to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
			sum(saldo_inicial+debe-haber)*-1 as monto
		from stage.stage_ingreso_historia sih
		where 1=1
		--and to_char(fecha_operacion, 'yyyy') = '2023'
		and posicion in ('4.3','4.4','4.5','4.6','4.7')
		group by posicion ,
		cuenta_resumen ,
		to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
		), activo_total as (
		select
			fecha_operacion,
			sum(monto) as monto
		from activo
		group by fecha_operacion
		), pasivo_total as (
		select
			fecha_operacion,
			sum(monto) as monto
		from pasivo
		group by fecha_operacion
		)
		select
			a.fecha_operacion,
			'liquidez' as resultado,
			a.monto-p.monto as monto
		from activo_total a
		inner join pasivo_total p on a.fecha_operacion = p.fecha_operacion;
	end if;
end $$;