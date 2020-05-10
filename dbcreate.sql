create database if not exists db_instrument;
create user if not exists 'instrument'@'localhost' identified by 'instrument';
grant all on db_instrument.* to 'instrument'@'localhost';

create table if not exists tbl_instrument(
	id                int          not null auto_increment,
    symbol            varchar(60)  not null unique,
    security          varchar(200) not null,
    gics              varchar(200) not null,
    gics_sub_industry varchar(200) not null,
    
    primary key(id)
);

create table if not exists tbl_market_data(
	id            int          not null auto_increment,
    instrument_id int          not null,
    type          varchar(100) not null,
    date          date         not null,
    open          float        not null,
    high          float        not null, 
    low           float        not null,
    close         float        not null,
    volume        float        not null,
    dividends     float        not null,
    stock_splits  float        not null,
    
    primary key(id),
    foreign key(instrument_id) references tbl_instrument(id),
    unique(instrument_id, type, date)
);
