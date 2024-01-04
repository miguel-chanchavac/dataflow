insert into stage.estado_financiero
with costos as (
select
	posicion ,
	cuenta_resumen ,
	'ingreso' as tipo_cuenta,
	to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
	sum(saldo_inicial+debe-haber) as monto
from stage.stage_ingreso_historia sih
where 1=1
--and to_char(fecha_operacion, 'yyyy') = '2023'
and posicion in ('4')
group by posicion ,
cuenta_resumen ,
to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
)
select * from costos;