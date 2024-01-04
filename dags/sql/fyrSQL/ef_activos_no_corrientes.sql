insert into stage.estado_financiero
with activo_no_corriente as (
select
	posicion ,
	cuenta_resumen ,
	'activos_no_corrientes' as tipo_cuenta,
	to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd') as fecha_operacion,
	sum(saldo_inicial+debe-haber)*-1 as monto
from stage.stage_ingreso_historia sih
where 1=1
--and to_char(fecha_operacion, 'yyyy') = '2023'
and posicion in ('1.1','1.2','1.2.1','1.3')
group by posicion ,
cuenta_resumen ,
to_date(to_char(fecha_operacion, 'yyyy-MM-')||'01', 'yyyy-MM-dd')
)
select * from activo_no_corriente;