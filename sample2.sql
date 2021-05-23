create table students (
    id int,
    primary key(id)
);

create table class (
    num int,
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

insert into students values(1);
insert into students values(2);

insert into class values(5);
insert into class values(6);

insert into enrolls values(1,'hihi',6);
insert into enrolls values(1,'hihello',5);
insert into enrolls values(2,'lec6',5);

select * from class, enrolls;