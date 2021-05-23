class SimpleDatabaseError(Exception):
    pass


class NoSuchTable(SimpleDatabaseError):
    def __str__(self):
        return "No such table"


class CharLengthError(SimpleDatabaseError):
    def __str__(self):
        return "Char length should be over 0"


class CreateTableError(SimpleDatabaseError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"Create table has failed: {self.msg}"


class DropTableError(SimpleDatabaseError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"Drop table has failed: {self.msg}"


class DropReferencedTableError(DropTableError):
    def __init__(self, table):
        super().__init__(f"'{table}' is referenced by other table")


class DuplicateColumnDefError(CreateTableError):
    def __init__(self):
        super().__init__("column definition is duplicated")


class DuplicatePrimaryKeyDefError(CreateTableError):
    def __init__(self):
        super().__init__("primary key definition is duplicated")


class ReferenceTypeError(CreateTableError):
    def __init__(self):
        super().__init__("foreign key references wrong type")


class ReferenceNonPrimaryKeyError(CreateTableError):
    def __init__(self):
        super().__init__("foreign key references non primary key column")


class ReferenceColumnExistenceError(CreateTableError):
    def __init__(self):
        super().__init__("foreign key references non existing column")


class ReferenceTableExistenceError(CreateTableError):
    def __init__(self):
        super().__init__("foreign key references non existing table")


class NonExistingColumnDefError(CreateTableError):
    def __init__(self, column):
        super().__init__(f"'{column}' does not exists in column definition")


class TableExistenceError(CreateTableError):
    def __init__(self):
        super().__init__("table with the same name already exists")


class InsertionError(SimpleDatabaseError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"Insertion has failed: {self.msg}"


class InsertTypeMismatchError(InsertionError):
    def __init__(self):
        super().__init__("Types are not matched")


class InsertColumnExistenceError(InsertionError):
    def __init__(self, column):
        super().__init__(f"'{column}' does not exist")


class InsertColumnNonNullableError(InsertionError):
    def __init__(self, column):
        super().__init__(f"'{column}' is not nullable")


class InsertDuplicatePrimaryKeyError(InsertionError):
    def __init__(self):
        super().__init__("Primary key duplication")


class InsertReferentialIntegrityError(InsertionError):
    def __init__(self):
        super().__init__("Referential integrity violation")


class SelectionError(SimpleDatabaseError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"Selection has failed: {self.msg}"


class SelectTableExistenceError(SelectionError):
    def __init__(self, table):
        super().__init__(f"'{table}' does not exist")

class SelectColumnResolveError(SelectionError):
    def __init__(self, column):
        super().__init__(f"fail to resolve '{column}'")


class WhereIncomparableError(SimpleDatabaseError):
    def __str__(self):
        return "Where clause try to compare incomparable values"


class WhereTableNotSpecified(SimpleDatabaseError):
    def __str__(self):
        return "Where clause try to reference tables which are not specified"

class WhereColumnNotExist(SimpleDatabaseError):
    def __str__(self):
        return "Where clause try to reference non existing column"


class WhereAmbiguousReference(SimpleDatabaseError):
    def __str__(self):
        return "Where clause contains ambiguous reference"
