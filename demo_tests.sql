/******************************************************************************/
-- truncate table gas_consumption;

-- install counter
execute gas_counter_operation_pkg.counter_operation(1000, sysdate, 11, 4, 'I');
-- read data
execute gas_counter_operation_pkg.counter_operation(1000, sysdate, 11, 239);
-- read data
execute gas_counter_operation_pkg.counter_operation(1000, sysdate, 11, 323);
-- decommiss counter
execute gas_counter_operation_pkg.counter_operation(1000, sysdate, 11, 324, 'D');

-- install another counter for this consumer:

execute gas_counter_operation_pkg.counter_operation(1001, sysdate, 10, 1, 'I');
-- read data
execute gas_counter_operation_pkg.counter_operation(1001, sysdate, 10, 30);
-- read data
execute gas_counter_operation_pkg.counter_operation(1001, sysdate, 10, 55);

select * from gas_consumption;

-- Wrong cases:

-- wrong customer
execute gas_counter_operation_pkg.counter_operation(1005, sysdate, 10, 4);

-- one more installation for existing customer
execute gas_counter_operation_pkg.counter_operation(1000, sysdate, 10, 4, 'I');

/******************************************************************************/
/******************************************************************************/

select * from consumers;
-- update consumers set consumer_type = 'COUNTER' where consumer_id = 5;
-- update consumers set consumer_type = 'REGULAR' where consumer_id = 6;
select * from gas_consumption;




