CREATE TABLE hosted_data (
id bigint unsigned not null,
time_created bigint not null,
title  varchar(25),
upload_status int not null,
text_data varchar(150),
link varchar(150),
PRIMARY KEY (id)
);