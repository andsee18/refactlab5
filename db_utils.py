# db_utils.py
import sqlite3

DB_NAME = "university_complex.db"

def db_connect():
    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except sqlite3.Error as e:
        print(f"DB Error: {e}")
        return None

def db_close(conn):
    if conn:
        conn.close()

def create_tables(conn):  # Corrected: Function now takes conn
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                StudentID INTEGER PRIMARY KEY,
                FirstName TEXT,
                LastName TEXT,
                MajorID INTEGER,
                CityID INTEGER,
                EnrollmentDate DATE
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Majors (
                MajorID INTEGER PRIMARY KEY,
                MajorName TEXT UNIQUE
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Courses (
                CourseID INTEGER PRIMARY KEY,
                CourseName TEXT,
                Credits INTEGER,
                DepartmentID INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Departments (
                DepartmentID INTEGER PRIMARY KEY,
                DepartmentName TEXT UNIQUE
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Enrollments (
                StudentID INTEGER,
                CourseID INTEGER,
                Grade REAL,
                EnrollmentDate DATE,
                PRIMARY KEY (StudentID, CourseID),
                FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
                FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS Cities (
                CityID INTEGER PRIMARY KEY,
                CityName TEXT UNIQUE
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Table Error: {e}")

def drop_tables(conn):
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS Students")
        c.execute("DROP TABLE IF EXISTS Majors")
        c.execute("DROP TABLE IF EXISTS Courses")
        c.execute("DROP TABLE IF EXISTS Departments")
        c.execute("DROP TABLE IF EXISTS Enrollments")
        c.execute("DROP TABLE IF EXISTS Cities")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Table Error: {e}")