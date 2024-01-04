insert into stage.estado_financiero
with margen_bruto_gasto_operacion as (
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
select
	posicion ,
	cuenta_resumen ,
	nombre_cuenta as tipo_cuenta,
	to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
	sum(saldo_inicial+debe-haber)*-1 as monto
from stage.stage_ingreso_historia sih
where 1=1
and posicion in ('13','6.1','6.2','10','9', '7')
group by posicion ,
cuenta_resumen,
nombre_cuenta
,to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
)
select
	'3.7' as posicion,
	'ganancia_perdida_acumulada_del_periodo' as cuenta_resumen,
	'capital_reserva' as tipo_cuenta,
	fecha_operacion,
	sum(monto) as monto
from margen_bruto_gasto_operacion
group by fecha_operacion;