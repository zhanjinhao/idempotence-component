create table t_idempotence_state_center
(
    id            int auto_increment
        primary key,
    namespace     varchar(50)                        not null comment '命名空间',
    prefix        varchar(50)                        not null comment '前缀',
    raw_key       varchar(100)                       not null comment 'rawKey',
    consume_mode  varchar(10)                        not null comment '消费模式',
    scenario      varchar(10)                        not null comment '消费场景',
    x_id          char(32)                           not null comment '消费全局ID',
    consume_state varchar(10)                        not null comment '消费状态',
    expire_time   datetime                           not null comment '过期时间',
    create_time   datetime default CURRENT_TIMESTAMP not null comment '创建时间',
    constraint t_idempotence_state_center_key
        unique (namespace, prefix, raw_key)
);

create index t_idempotence_state_center_exp
    on t_idempotence_state_center (expire_time);

create table t_idempotence_state_center_his
(
    id            int auto_increment
        primary key,
    namespace     varchar(50)                        not null comment '命名空间',
    prefix        varchar(50)                        not null comment '前缀',
    raw_key       varchar(100)                       not null comment 'rawKey',
    consume_mode  varchar(10)                        not null comment '消费模式',
    scenario      varchar(10)                        not null comment '消费场景',
    x_id          char(32)                           not null comment '全局消费ID',
    consume_state varchar(10)                        not null comment '消费状态',
    expire_time   datetime                           not null comment '过期时间',
    create_time   datetime                           not null comment '创建时间',
    delete_time   datetime default CURRENT_TIMESTAMP not null comment '删除时间'
);

create index t_idempotence_state_center_his_key
    on t_idempotence_state_center_his (namespace, prefix, raw_key);

create table t_idempotence_exception_log
(
    id              int auto_increment
        primary key,
    namespace       varchar(50)                        not null comment '命名空间',
    prefix          varchar(50)                        not null comment '前缀',
    raw_key         varchar(100)                       not null comment 'rawKey',
    consume_mode    varchar(10)                        not null comment '消费模式',
    x_id            char(32)                           not null comment '消费全局ID',
    consume_stage   varchar(50)                        not null comment '消费阶段',
    scenario        varchar(10)                        not null comment '业务场景：REQUEST、MQ',
    args            longtext                           null comment '參數',
    exception_msg   longtext                           null comment '异常信息',
    exception_stack longtext                           null comment '异常栈',
    expire_time     datetime                           not null comment '过期时间',
    create_time     datetime default CURRENT_TIMESTAMP null comment '创建时间'
);

create index t_idempotence_exception_log_key
    on t_idempotence_exception_log (namespace, prefix, raw_key);

create table t_idempotence_state_center_bak
(
    id            int auto_increment
        primary key,
    namespace     varchar(50)                        not null comment '命名空间',
    prefix        varchar(50)                        not null comment '前缀',
    raw_key       varchar(100)                       not null comment 'rawKey',
    consume_mode  varchar(10)                        not null comment '消费模式',
    scenario      varchar(10)                        not null comment '消费场景',
    x_id          char(32)                           not null comment '消费全局ID',
    consume_state varchar(10)                        not null comment '消费状态',
    expire_time   datetime                           not null comment '过期时间',
    create_time   datetime default CURRENT_TIMESTAMP not null comment '创建时间',
    constraint t_idempotence_state_center_bak_key
        unique (namespace, prefix, raw_key)
);

create index t_idempotence_state_center_bak_exp
    on t_idempotence_state_center_bak (expire_time);

create table t_idempotence_state_center_his_bak
(
    id            int auto_increment
        primary key,
    namespace     varchar(50)                        not null comment '命名空间',
    prefix        varchar(50)                        not null comment '前缀',
    raw_key       varchar(100)                       not null comment 'rawKey',
    consume_mode  varchar(10)                        not null comment '消费模式',
    scenario      varchar(10)                        not null comment '消费场景',
    x_id          char(32)                           not null comment '全局消费ID',
    consume_state varchar(10)                        not null comment '消费状态',
    expire_time   datetime                           not null comment '过期时间',
    create_time   datetime                           not null comment '创建时间',
    delete_time   datetime default CURRENT_TIMESTAMP not null comment '删除时间'
);

create index t_idempotence_state_center_his_bak_key
    on t_idempotence_state_center_his_bak (namespace, prefix, raw_key);

create table t_idempotence_exception_log_bak
(
    id              int auto_increment
        primary key,
    namespace       varchar(50)                        not null comment '命名空间',
    prefix          varchar(50)                        not null comment '前缀',
    raw_key         varchar(100)                       not null comment 'rawKey',
    consume_mode    varchar(10)                        not null comment '消费模式',
    x_id            char(32)                           not null comment '消费全局ID',
    consume_stage   varchar(50)                        not null comment '消费阶段',
    scenario        varchar(10)                        not null comment '业务场景：REQUEST、MQ',
    args            longtext                           null comment '參數',
    exception_msg   longtext                           null comment '异常信息',
    exception_stack longtext                           null comment '异常栈',
    expire_time     datetime                           not null comment '过期时间',
    create_time     datetime default CURRENT_TIMESTAMP null comment '创建时间'
);

create index t_idempotence_exception_log_bak_key
    on t_idempotence_exception_log_bak (namespace, prefix, raw_key);
