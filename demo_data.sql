-- Fill Test Data

--------------------------------------------------------------------------------
-- 1)

-- truncate table consumers;

-- create consumer who has no counter
insert into consumers (consumer_id, consumer_name, consumer_addr, consumer_type, consumer_cat, consumer_phone, consumer_long, consumer_lat) 
values (1000, 'Demo Customer 1', 'Germany, Backnang, Talstrasse 45', 'REGULAR', 'BUSINESS', '+49 7191 907990', 48.949506, 9.429773);


-- create consumer who has counter
insert into consumers (consumer_id, consumer_name, consumer_addr, consumer_type, consumer_cat, consumer_phone, consumer_long, consumer_lat) 
values (1001, 'Demo Customer 2', 'Germany, Backnang, Talstrasse 57', 'REGULAR', 'PRIVATE', '+49 7112 234567', 48.949119, 9.431290);

commit;

select * from consumers;

--------------------------------------------------------------------------------
-- 2) Counters
insert into counters (counter_id, counter_model, counter_manufacturer, counter_max_capacity, gsm_module)
values (10, 'NB-IoT Smart Gas Meter', 'GoldStar Smart Group', 4, 'Y');


insert into counters (counter_id, counter_model, counter_manufacturer, counter_max_capacity, gsm_module)
values (11, 'EN 1359', 'Hangzhou Laison Tech', 2.5, 'N');


insert into counters_numbers (counter_id, gms_counter_number) values (10, '+49 1234 5678901');
commit;

select * from counters;

/******************************************************************************/
/******************************************************************************/
