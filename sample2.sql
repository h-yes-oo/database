create table students (
    id int,
    birthday date,
    primary key(id)
);

create table class (
    num int,
    start_date date,
    primary key(num)
);

create table enrolls (
    id int,
    lecture_name char(50),
    num int,
    primary key(lecture_name),
    foreign key(id) references students(id),
    foreign key(num) references class(num)
);

insert into students values(1, null);
insert into students values(2, 2020-02-16);
insert into students values(3, 2020-03-16);
insert into students values(4, 2020-04-16);
insert into students values(5, 2020-05-16);
insert into students values(6, 2020-06-16);

insert into class values(5, null);
insert into class values(6, null);
insert into class values(3, 2020-03-16);
insert into class values(4, 2020-03-17);

insert into enrolls values(1,'hihi',6);
insert into enrolls values(1,'hihello',5);
insert into enrolls values(2,'lec6',5);
insert into enrolls values(3,'hihi3',6);
insert into enrolls values(4,'hi44',3);
insert into enrolls values(5,'leadfa',4);

select * from account;

create table account (
    ACCOUNT_NUMBER date,
    branch_name char(20),
    BALANCE int,
    primary key(account_number)
);

create table loan (
    ACCOUNT_NUMBER date,
    branch_name char(20),
    BALANCE int,
    primary key(account_number)
);
