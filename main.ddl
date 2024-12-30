drop table public.aiai_stg_transactions cascade;
drop table public.aiai_dwh_fact_transactions cascade;
drop table public.aiai_stg_terminals cascade;
drop table public.aiai_dwh_dim_terminals_hist cascade;
drop table public.aiai_stg_passport_blacklist cascade;
drop table public.aiai_dwh_fact_passport_blacklist cascade;
drop table public.aiai_stg_clients cascade;
drop table public.aiai_dwh_dim_clients_hist cascade;
drop table public.aiai_stg_accounts cascade;
drop table public.aiai_dwh_dim_accounts_hist cascade;
drop table public.aiai_stg_cards cascade;
drop table public.aiai_dwh_dim_cards_hist cascade;
drop table public.aiai_rep_fraud cascade;

--truncate table public.aiai_stg_transactions cascade;
--truncate table public.aiai_dwh_fact_transactions cascade;
--truncate table public.aiai_stg_terminals cascade;
--truncate table public.aiai_dwh_dim_terminals_hist cascade;
--truncate table public.aiai_stg_passport_blacklist cascade;
--truncate table public.aiai_dwh_fact_passport_blacklist cascade;
--truncate table public.aiai_stg_clients cascade;
--truncate table public.aiai_stg_accounts cascade;
--truncate table public.aiai_stg_cards cascade;
--truncate table public.aiai_dwh_dim_cards_hist cascade;
--truncate table public.aiai_dwh_dim_accounts_hist cascade;
--truncate table public.aiai_dwh_dim_clients_hist cascade;
--truncate table public.aiai_rep_fraud cascade;

create table aiai_stg_transactions (
trans_id varchar,
trans_date timestamp(0),
card varchar,
oper_type varchar,
amt decimal,
oper_result varchar,
terminal varchar
--FOREIGN KEY (card) REFERENCES aiai_stg_cards(card),
--FOREIGN KEY (terminal) REFERENCES aiai_stg_terminals(terminal_id)
);

create table aiai_dwh_fact_transactions (
trans_id varchar,
trans_date timestamp(0),
card varchar,
oper_type varchar,
amt decimal,
oper_result varchar,
terminal varchar
--FOREIGN KEY (card) REFERENCES aiai_stg_cards(card),
--FOREIGN KEY (terminal) REFERENCES aiai_stg_terminals(terminal_id)
);

create table aiai_stg_terminals (
terminal_id varchar,
terminal_type varchar,
terminal_city varchar,
terminal_address varchar
);

create table aiai_stg_passport_blacklist (
passport_num varchar,
entry_dt date
);
create table aiai_stg_clients (
client_id varchar,
last_name varchar,
first_name varchar,
patronymic varchar,
date_of_birth date,
passport_num varchar,
passport_valid_to date,
phone varchar,
create_dt timestamp(0),
update_dt timestamp(0)
);
create table aiai_stg_accounts (
account varchar,
valid_to date,
client varchar,
create_dt timestamp(0),
update_dt timestamp(0)
--foreign key (client) references aiai_stg_clients(client_id)
);
create table aiai_stg_cards (
card varchar,
account varchar,
create_dt timestamp(0),
update_dt timestamp(0)
--foreign key (account) references aiai_stg_accounts(account)
);

create table aiai_dwh_dim_clients_hist (
client_id varchar,
last_name varchar,
first_name varchar,
patronymic varchar,
date_of_birth date,
passport_num varchar,
passport_valid_to date,
phone varchar,
effective_from timestamp(0),
effective_to timestamp(0),
deleted_flg bool
);

create table aiai_dwh_dim_accounts_hist (
account varchar,
valid_to date,
client varchar,
create_dt timestamp(0),
update_dt timestamp(0)
--effective_from timestamp(0),
--effective_to timestamp(0),
--deleted_flg bool
--foreign key (client) references aiai_stg_clients(client_id)
);

create table aiai_dwh_dim_terminals_hist (
terminal_id varchar,
terminal_type varchar,
terminal_city varchar,
terminal_address varchar,
effective_from timestamp(0),
effective_to timestamp(0),
deleted_flg bool
);

create table aiai_dwh_dim_cards_hist (
card varchar,
account varchar,
create_dt timestamp(0),
update_dt timestamp(0)
--effective_from timestamp(0),
--effective_to timestamp(0),
--deleted_flg bool
--foreign key (account) references aiai_stg_accounts(account)
);

create table aiai_dwh_fact_passport_blacklist (
passport_num varchar,
entry_dt date
);


create table aiai_rep_fraud (
event_dt timestamp(0),
passport varchar,
fio varchar,
phone varchar,
event_type varchar,
report_dt date
);


--with t4 as (
--with t3 as (
--with t2 as (
--with t1 as (
--select tr.trans_id, tr.card, tr.trans_date, tr.oper_type, tr.amt, tr.oper_result, tr.terminal, crd.account, crd.create_dt, crd.update_dt from aiai_dwh_fact_transactions tr
--left join aiai_dwh_dim_cards_hist crd  on tr.card = crd.card)
--select trans_id, card, trans_date, oper_type, amt, oper_result, terminal, t1.account, t1.create_dt as crd_create_dt, acc.client, acc.valid_to as acc_valid_to, acc.create_dt as acc_create_dt  from t1
--left join aiai_dwh_dim_accounts_hist acc on t1.account = acc.account)
--select trans_id, card, trans_date, oper_type, amt, oper_result, terminal, t2.account, t2.crd_create_dt, t2.client, t2.acc_valid_to, t2.acc_create_dt, cl.last_name, cl.first_name, cl.patronymic, cl.date_of_birth, cl.passport_num, cl.passport_valid_to, cl.phone  from t2
--left join aiai_dwh_dim_clients_hist cl on t2.client = cl.client_id)
--select trans_id, card, trans_date, oper_type, amt, oper_result, t3.terminal, account, crd_create_dt, client, acc_valid_to, acc_create_dt, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, term.terminal_type, term.terminal_city, term.terminal_address from t3
--left join aiai_dwh_dim_terminals_hist term on t3.terminal = term.terminal_id)
--select *, rank() over (partition by cast(trans_date as date), client, trans_id order by trans_date asc) from t4
--t5 as (
--select trans_id, 'Заблокированный или просроченный паспорт' as event_type, card, trans_date, oper_type, amt, oper_result, terminal, account, crd_create_dt, client, acc_valid_to, acc_create_dt, last_name, first_name, patronymic, date_of_birth, t4.passport_num, passport_valid_to, phone, terminal_type, terminal_city, terminal_address from t4, public.aiai_dwh_fact_passport_blacklist bl
--where cast(t4.trans_date as date) >= bl.entry_dt and t4.passport_num = bl.passport_num)
--select * from t5

--where passport_num in (select passport_num from public.aiai_dwh_fact_passport_blacklist bl) and cast('trans_date' as date) 

