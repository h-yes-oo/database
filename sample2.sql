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
insert into students values(3);
insert into students values(4);
insert into students values(5);
insert into students values(6);

insert into class values(5);
insert into class values(6);

insert into enrolls values(1,'hihi',6);
insert into enrolls values(1,'hihello',5);
insert into enrolls values(2,'lec6',5);
insert into enrolls values(3,'hihi3',6);
insert into enrolls values(4,'hi44',5);
insert into enrolls values(5,'leadfa',5);

select * from class, enrolls;

('and', [('or', [(True, ('a', 'is not', None)), (True, ('b', 'is', None)), (True, [('and', [('or', [(True, (('col', 'c'), '>', ('int', 3)))]), ('or', [(True, (('date', datetime.datetime(2020, 3, 5, 0, 0)), '>', ('col', 'da')))])])])])])

 select * from enrolls, students where num > 5 and ( not lecture_name > 'abc' or enrolls.id < 5) or students.id is null;
['and', [['or', [(('col', 'num'), '>', ('int', 5)), [['and', [['or', [['not', (('col', 'lecture_name'), '>', ('str', 'abc'))]]], ['or', [(('col', 'enrolls.id'), '<', ('int', 5))]]]]]]], ['or', [('students.id', 'is', None)]]]]

['and', [['or', [(('col', 'num'), '>', ('int', 5))]]]]
