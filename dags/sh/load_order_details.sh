###################################################
#create by: Miguel Angel Chanchavac Alvarado
#createdon Date: 20210510
#Running copy command for load order_details table
###################################################
#!/bin/bash

echo "###-----Start process--------------------------------------"

path_file="/Users/mangeluz/Documents/Laboratorio/applaudostudios/output"

echo "###-----Verified files Path--------------------------------"
echo "###--------------------------------------------------------"
conteo=`ls ${path_file}/stg_order/part*.csv | wc -l`
file=`ls ${path_file}/stg_order/part*.csv`
valida='pending'

echo "###-----Validated if exists files in path------------------"
echo "${file}"
echo "###--------------------------------------------------------"
while [ ${conteo} -ge 0 ]
do
  echo "###-----Truncate table in database-------------------------"
  echo "###--------------------------------------------------------"
  psql -h localhost -U postgres -d test -c "truncate table stage.stg_order_details"

  for x in ${file}
  do
    psql -h localhost -U postgres -d test -c "\copy stage.stg_order_details FROM '${x}' with (format csv, header true, delimiter ',');"

    if [ $? -eq 0 ]
    then
      valida='done'
    else
      echo "###-----exist an error in this execution-------------------"
      echo "###-----Process ended with error---------------------------"
      exit 1
    fi

  done

  if [ ${valida} == 'done' ]
  then
    echo "###-----executed copy command in database------------------"
    echo "###--------------------------------------------------------"
    echo "###-----End process----------------------------------------"
    exit
  fi

  sleep 5

  ####Try again
  conteo=`ls ${path_file}/stg_order/part*.csv | wc -l`
  file=`ls ${path_file}/stg_order/part*.csv`
done
