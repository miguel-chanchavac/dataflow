insert into stage.estado_financiero
with capital as (
select
	posicion ,
	cuenta_resumen ,
	'capital_reserva' as tipo_cuenta,
	to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
	sum(saldo_inicial+debe-haber)*-1 as monto
from stage.stage_ingreso_historia sih
where 1=1
--and to_char(fecha_operacion, 'yyyy') = '2023'
and posicion in ('3.1','3.3','3.2','3.4','3.5','3.6')
group by posicion ,
cuenta_resumen ,
to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
)
select * from capital