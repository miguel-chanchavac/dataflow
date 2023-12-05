###################################################
#create by: Miguel Angel Chanchavac Alvarado
#createdon Date: 20210510
#Running copy command for unload order_details table
###################################################
set -x

psql -h localhost -U postgres -d test -c "\copy (select * from cube_orders) to /Users/mangeluz/Documents/miguelangel/CV/applaudoStudios/output/datamart/cube_orders.csv with (format csv, header true, delimiter ',');"
