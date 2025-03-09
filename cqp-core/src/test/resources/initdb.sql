drop table if exists task_status;

create table task_status(
    id uuid primary key,
    status varchar,
    task_type varchar,
    restart_flag int,
    flow_id varchar,
    deserializer varchar,
    created_by varchar,
    created_date timestamp without time zone,
    updated_date timestamp without time zone,
    state varchar,
    parallelism int,
    buffer int
);