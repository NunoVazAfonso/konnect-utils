use default_schema; 

create table test_table (id int  NOT NULL AUTO_INCREMENT, name varchar(255), primary key(id) );

insert into test_table(name) values("teste um");
insert into test_table(name) values("teste dois");