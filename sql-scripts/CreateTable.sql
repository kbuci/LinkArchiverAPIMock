CREATE TABLE hosted_data (
id bigint unsigned not null,
time_expired bigint not null,
upload_status int not null,
text_data varchar(150),
link_archive varchar(50),
link varchar(150),
PRIMARY KEY (id)
);

CREATE TABLE domain_polling (
    domain varchar(150) not null,
    last_polled bigint,
    poll_delay int,
    PRIMARY KEY (domain)
)