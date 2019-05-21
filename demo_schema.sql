/*
  drop sequence operation_id_seq;

  drop table gas_consumption;
  drop table counters_numbers;
  drop table counters;
  drop table consumers;
*/

--------------------------------------------------------------------------------
-- 1)
/*
  consumer_type
  REGULAR - without gas counter
  COUNTER - has installed gas counter
*/

create sequence operation_id_seq increment by 1 start with 1 cache 1000;

create table consumers (
  consumer_id number primary key,
  consumer_name varchar2(100) not null,
  consumer_addr varchar2(100),
  consumer_type varchar2(10) not null,
  consumer_cat varchar2(8) not null,
  consumer_phone varchar2(50),
  consumer_long number,
  consumer_lat number,
  constraint consumer_type_chk check (consumer_type in ('REGULAR', 'COUNTER')) enable,
  constraint consumer_cat_chk check (consumer_cat in ('PRIVATE', 'BUSINESS')) enable
);

--------------------------------------------------------------------------------
-- 2)

create table counters (
  counter_id number not null primary key,
  counter_model varchar2(50) not null,
  counter_manufacturer varchar2(35),
  counter_max_capacity number not null,
  gsm_module char(1) not null,
  constraint gsm_module_chk check (gsm_module in ('Y', 'N')) enable
);

comment on column counters.counter_max_capacity is 'MAX counter bandwidth, M3/h';

--------------------------------------------------------------------------------
--3) 

create table counters_numbers (
  counter_id number,
  gms_counter_number varchar2(20), 
  constraint counter_id_num_fk foreign key (counter_id) references counters(counter_id) enable
);

--------------------------------------------------------------------------------
-- 3)
-- drop table gas_consumption;

create table gas_consumption (
  operation_id number primary key,
  consumer_id number,
  --
  operation_type char(1) not null,
  operation_date date,
  -- 
  counter_id number not null,
  counter_data number not null,
  gas_consumed number not null,
  -- 
  is_suspected char(1) default 'N',
  --
  user_name varchar2(100),
  change_date date
  --  
  , constraint cons_operation_type_uk unique (consumer_id, operation_type, operation_date, counter_id, counter_data)
  , constraint consumer_id_fk foreign key (consumer_id) references consumers(consumer_id)
  , constraint counter_id_fk foreign key (counter_id) references counters(counter_id)
  , constraint operation_type_chk check (operation_type in ('I', 'R', 'D'))  
);

--------------------------------------------------------------------------------
-- 4) Logic:

/*
  operation_type:
  I - meter installation
  R - meter data read
  D - meter decommission
  
  business rules for DEMO:
  1. We can install counter only to those customer who has no counter
  2. If customer has no counters, we cannot get measurements from it
  3. We can read measurements only from installed counter
  4. Counter can be decommissioned only after installation  
  5. Consumed gas (i.e. m3) is the difference between current and previous data
  6. Consumed gas for some period should not exceed MAX bandwidth of counter
    exsample: 
      counter max bandwidth is 5 m3/h - i.e. 24*5 = 120 m3 per day. 
      counsumer reported data is 1300 m3 per 10 days - i.e. 130 m3 per 1 day.
      in this case counter data should be marked as suspected (probably, operator made some typo)
*/

-- ########################################################################## --
-- ########################################################################## --
-- ########################################################################## --

create or replace package gas_counter_operation_pkg as 

  wrong_data_error                exception;

  counter_not_installed_error     exception;
  counter_already_installed_error exception;
  counter_data_error              exception;
  counter_bandwidth_error         exception;
  counter_wrong_type_error        exception; 
  counter_wrong_operation_error   exception;
  
  consumer_not_exists_error       exception;
  counter_not_exists_error        exception;
  
  procedure counter_operation(p_consumer_id number, p_operation_date date, p_counter_id number, p_counter_data number, p_operation char default 'R');

end gas_counter_operation_pkg;

create or replace package body gas_counter_operation_pkg as 
  ------------------------------------------------------------------------------
  procedure counter_operation(p_consumer_id number, p_operation_date date, 
        p_counter_id number, p_counter_data number, p_operation char default 'R') as 
  
    v_consumer_type         consumers.consumer_type%type;  
    v_counter_id            gas_consumption.counter_id%type;    
    v_counter_max_capacity  counters.counter_max_capacity%type;
    
    v_consumed_gas_volume   number;
    v_consumption_period    number;
    
    r_gas_consumption_prev  gas_consumption%rowtype;    
    
  begin
  
    if p_consumer_id is null or 
       p_counter_id is null or 
       p_counter_data is null or 
       p_counter_data < 0 or 
       p_operation not in ('I', 'R', 'D') then
      raise wrong_data_error;
    end if;
  
    select consumer_type into v_consumer_type from consumers where consumer_id = p_consumer_id;
    select counter_max_capacity into v_counter_max_capacity from counters where counter_id = p_counter_id;
    
    savepoint cnt_operation;    
    
    -- counter installation
    if p_operation = 'I' then
      
      if v_consumer_type = 'COUNTER' then
        raise counter_already_installed_error;
      else      
        update consumers set consumer_type = 'COUNTER' where consumer_id = p_consumer_id;
        
        insert into gas_consumption (operation_id, consumer_id, operation_type, operation_date, counter_id, counter_data, gas_consumed, user_name, change_date)
        select 
          operation_id_seq.nextval operation_id,
          p_consumer_id consumer_id, 
          p_operation operation_type, 
          p_operation_date operation_date, 
          p_counter_id counter_id, 
          p_counter_data counter_data, 
          0 gas_consumed, 
          user user_name, 
          sysdate change_date
        from dual;        
      end if;
      
    -- counter read and decommission:
    else
    
      if v_consumer_type = 'REGULAR' then
        raise counter_not_installed_error;
      else      
        -- get ID of last installed counter for this consumer
        select counter_id into v_counter_id from gas_consumption g where consumer_id = p_consumer_id 
          and operation_date = (
            select max(operation_date) from gas_consumption gg 
            where gg.consumer_id = g.consumer_id and operation_type = 'I'
          );
      
        if v_counter_id != p_counter_id then
          raise counter_wrong_type_error;
        else
        
          select * into r_gas_consumption_prev from gas_consumption g where consumer_id = p_consumer_id 
          and operation_date = (
            select max(operation_date) from gas_consumption gg 
            where gg.consumer_id = g.consumer_id and operation_type in ('I', 'R')
          );        
          
          if ( r_gas_consumption_prev.operation_date < p_operation_date and r_gas_consumption_prev.counter_data > p_counter_data ) or 
             ( r_gas_consumption_prev.operation_date > p_operation_date and r_gas_consumption_prev.counter_data < p_counter_data ) then
             raise counter_data_error;
          else            
          
            v_consumed_gas_volume := p_counter_data - r_gas_consumption_prev.counter_data;
            -- consumption period in hours
            v_consumption_period := (p_operation_date - r_gas_consumption_prev.operation_date) * 24;
            
            --------------------------------------------------------------------
            -- main block
            --------------------------------------------------------------------
            
            -- decommission of counter
            if p_operation = 'D' then
              update consumers set consumer_type = 'REGULAR' where consumer_id = p_consumer_id; 
            end if;
            
            -- add record with gas volume
            insert into gas_consumption (operation_id, consumer_id, operation_type, operation_date, counter_id, counter_data, gas_consumed, is_suspected, user_name, change_date)
            select 
              operation_id_seq.nextval operation_id,
              p_consumer_id consumer_id, 
              p_operation operation_type, 
              p_operation_date operation_date, 
              p_counter_id counter_id, 
              p_counter_data counter_data, 
              v_consumed_gas_volume gas_consumed, 
              case 
                when v_consumed_gas_volume / v_consumption_period > v_counter_max_capacity then 'Y' 
                else 'N'
              end is_suspected,
              user user_name, 
              sysdate change_date
            from dual;
            
          end if;        
        end if;
      end if;
    end if;
    exception
    
      when no_data_found then
        rollback to cnt_operation;
        raise_application_error (num => -20090, msg => 'Consumer with ID=' || 
                                to_char(p_consumer_id) || ' was not found');
      when counter_already_installed_error then
        rollback to cnt_operation;
        raise_application_error (num => -20091, msg => 'Consumer with ID=' || 
                                to_char(p_consumer_id) || ' has installed gas counter, make decommission of old one first');
      when counter_not_installed_error then
        rollback to cnt_operation;
        raise_application_error (num => -20092, msg => 'Consumer with ID=' || 
                                to_char(p_consumer_id) || ' has no installed gas counter, make installation first');      
      when counter_data_error then
        rollback to cnt_operation;
        raise_application_error (num => -20093, msg => 'Counter data error for Consumer with ID=' || to_char(p_consumer_id));
      
      when counter_wrong_type_error then
        rollback to cnt_operation;
        raise_application_error (num => -20094, msg => 'Wrong counter type for Consumer with ID=' || to_char(p_consumer_id));
        
      when wrong_data_error then
        rollback to cnt_operation;  
        raise_application_error (num => -20098, msg => 'Data error for counter operation. Check input parameters');  
        
      when others then
        rollback to cnt_operation;
        raise_application_error (num => -20099, msg => 'Error happened while operating counter for consumer with ID=' || 
                                to_char(p_consumer_id) || '; Error code: ' || to_char(sqlcode) || ', error message:' || sqlerrm );
  end;
end gas_counter_operation_pkg;