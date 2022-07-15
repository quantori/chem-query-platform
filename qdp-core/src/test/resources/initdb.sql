create table task_statuses(
    id uuid primary key,
    status varchar,
    type varchar,
    restart_flag int,
    flow_id varchar,
    deserializer varchar,
    created_by varchar,
    created_date date not null,
    updated_date date not null,
    state varchar,
    parallelism int,
    buffer int
);