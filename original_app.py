import sqlite3
import time

# Подключение к БД
def connect_to_db(db_name="university_complex.db"):
    conn = sqlite3.connect(db_name)
    return conn

# Создание таблиц
def create_tables(conn):
    cursor = conn.cursor()

    # Таблица Students
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Students (
        StudentID INTEGER PRIMARY KEY AUTOINCREMENT,
        FirstName TEXT,
        LastName TEXT,
        MajorID INTEGER,
        CityID INTEGER,
        EnrollmentDate DATE
    )
    ''')

    # Таблица Majors
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Majors (
        MajorID INTEGER PRIMARY KEY AUTOINCREMENT,
        MajorName TEXT
    )
    ''')

    # Таблица Cities
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Cities (
        CityID INTEGER PRIMARY KEY AUTOINCREMENT,
        CityName TEXT
    )
    ''')

    # Таблица Courses
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Courses (
        CourseID INTEGER PRIMARY KEY AUTOINCREMENT,
        CourseName TEXT,
        Credits INTEGER,
        DepartmentID INTEGER
    )
    ''')

    # Таблица Departments
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Departments (
        DepartmentID INTEGER PRIMARY KEY AUTOINCREMENT,
        DepartmentName TEXT
    )
    ''')

    # Таблица Enrollments
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Enrollments (
        EnrollmentID INTEGER PRIMARY KEY AUTOINCREMENT,
        StudentID INTEGER,
        CourseID INTEGER,
        Grade REAL,
        EnrollmentDate DATE,
        FOREIGN KEY (StudentID) REFERENCES Students(StudentID),
        FOREIGN KEY (CourseID) REFERENCES Courses(CourseID)
    )
    ''')
    conn.commit()

# Заполнение таблиц данными
def populate_tables(conn, num_students=2000, num_majors=10, num_cities=20, num_courses=50, num_departments=5, num_enrollments=10000):
    cursor = conn.cursor()
    import random
    import datetime

    # Majors
    for i in range(num_majors):
        cursor.execute("INSERT INTO Majors (MajorName) VALUES (?)", (f"Major{i}",))

    # Cities
    for i in range(num_cities):
        cursor.execute("INSERT INTO Cities (CityName) VALUES (?)", (f"City{i}",))

    # Departments
    for i in range(num_departments):
        cursor.execute("INSERT INTO Departments (DepartmentName) VALUES (?)", (f"Department{i}",))

    conn.commit()

    # Courses
    for i in range(num_courses):
        dept_id = random.randint(1, num_departments)
        cursor.execute("INSERT INTO Courses (CourseName, Credits, DepartmentID) VALUES (?, ?, ?)",
                       (f"Course{i}", 3 + i % 3, dept_id))

    conn.commit()

    # Students
    for i in range(num_students):
        major_id = random.randint(1, num_majors)
        city_id = random.randint(1, num_cities)
        enrollment_date = datetime.date(2020 + random.randint(0, 3), random.randint(1, 12), random.randint(1, 28))
        cursor.execute(
            "INSERT INTO Students (FirstName, LastName, MajorID, CityID, EnrollmentDate) VALUES (?, ?, ?, ?, ?)",
            (f"FirstName{i}", f"LastName{i}", major_id, city_id, enrollment_date)
        )
    conn.commit()

    # Enrollments
    for _ in range(num_enrollments):
        student_id = random.randint(1, num_students)
        course_id = random.randint(1, num_courses)
        grade = random.uniform(0, 4)
        enrollment_date = datetime.date(2020 + random.randint(0, 3), random.randint(1, 12), random.randint(1, 28))
        cursor.execute(
            "INSERT INTO Enrollments (StudentID, CourseID, Grade, EnrollmentDate) VALUES (?, ?, ?, ?)",
            (student_id, course_id, grade, enrollment_date)
        )
    conn.commit()

# Примеры неоптимизированных запросов
def get_students_from_city_no_index(conn, city_name):
    cursor = conn.cursor()
    cursor.execute(f'''
    SELECT s.* FROM Students s
    JOIN Cities c ON s.CityID = c.CityID
    WHERE c.CityName = '{city_name}'
    ''') # Нет индекса, SELECT *
    return cursor.fetchall()

def get_courses_in_department_inefficient(conn, dept_name):
    cursor = conn.cursor()
    cursor.execute(f'''
    SELECT c.* FROM Courses c
    WHERE c.DepartmentID IN (SELECT DepartmentID FROM Departments WHERE DepartmentName = '{dept_name}')
    ''') # Подзапрос
    return cursor.fetchall()

def get_student_enrollments_with_details_complex_inefficient(conn):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT s.*, m.*, c.*, e.*, d.*, city.*
    FROM Students s
    JOIN Majors m ON s.MajorID = m.MajorID
    JOIN Enrollments e ON s.StudentID = e.StudentID
    JOIN Courses c ON e.CourseID = c.CourseID
    JOIN Departments d ON c.DepartmentID = d.DepartmentID
    JOIN Cities city ON s.CityID = city.CityID
    ''') # Очень много JOIN'ов, SELECT *
    return cursor.fetchall()

def get_average_grade_per_major_inefficient(conn):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT m.MajorName, AVG(e.Grade)
    FROM Majors m
    JOIN Students s ON m.MajorID = s.MajorID
    JOIN Enrollments e ON s.StudentID = e.StudentID
    GROUP BY m.MajorName
    ''') #  Нет индексов для агрегации
    return cursor.fetchall()

def main():
    conn = connect_to_db()
    create_tables(conn)
    populate_tables(conn, num_students=2000, num_majors=10, num_cities=20, num_courses=50, num_departments=5,
                    num_enrollments=10000)

    # Замеры времени и вывод результатов
    start_time = time.time()
    students = get_students_from_city_no_index(conn, "City5")
    print(f"Время выполнения get_students_from_city_no_index: {time.time() - start_time}")
    print(f"Найдено студентов: {len(students)}")

    start_time = time.time()
    courses = get_courses_in_department_inefficient(conn, "Department1")
    print(f"Время выполнения get_courses_in_department_inefficient: {time.time() - start_time}")
    print(f"Найдено курсов: {len(courses)}")

    start_time = time.time()
    enrollments_details = get_student_enrollments_with_details_complex_inefficient(conn)
    print(f"Время выполнения get_student_enrollments_with_details_complex_inefficient: {time.time() - start_time}")
    print(f"Найдено записей: {len(enrollments_details)}")

    start_time = time.time()
    avg_grades = get_average_grade_per_major_inefficient(conn)
    print(f"Время выполнения get_average_grade_per_major_inefficient: {time.time() - start_time}")
    print(f"Средние баллы: {avg_grades}")

    conn.close()

if __name__ == "__main__":
    main()