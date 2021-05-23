create table department (
    name char(5),
    pos char(17),
    number int,
    start date,
    primary key(name)
);


create table student (
    ID char(5),
    name char(2) not null,
    dept_name char(5),
    tot_cred int,
    primary key(ID, name),
    foreign key (dept_name) references department(name)
);

create table lover (
    x char(3),
    y char(2),
    dept_name char(5),
    stu_id char(5),
    primary key(x,y,dept_name),
    foreign key (dept_name) references department(name),
    foreign key (y,stu_id) references student(name,id)
);

create table loves (
    she char(2),
    he char(3) not null,
    who char(5),
    love date,
    primary key (love),
    foreign key (who,he,she) references lover(dept_name,x,y)
);

insert into department (number,start,pos,name) values(1,2020-10-10,"pos777777",'name5555555');


insert into student values('id1','n2','name5',10);

insert into lover values('xxx','n2','name5','id1');

insert into loves values('n2','xxx','name5',2015-03-30);

--set null value to primary key
--set null value to not null primary key
--len(column) != len(input_column)
--len(input_column) != len(values)
