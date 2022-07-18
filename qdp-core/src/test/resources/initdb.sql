drop table if exists task_statuses;

create table task_statuses(
    id uuid primary key,
    status varchar,
    type varchar,
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