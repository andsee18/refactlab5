# data_generator.py
import sqlite3
import random
from datetime import date, timedelta

def generate_cities(conn, num_cities=10):
    c = conn.cursor()
    c.executemany("INSERT INTO Cities (CityName) VALUES (?)", [(f"City{i}",) for i in range(1, num_cities + 1)])

def generate_majors(conn, num_majors=5):
    c = conn.cursor()
    c.executemany("INSERT INTO Majors (MajorName) VALUES (?)", [(f"Major{i}",) for i in range(1, num_majors + 1)])

def generate_departments(conn, num_departments=3):
    c = conn.cursor()
    c.executemany("INSERT INTO Departments (DepartmentName) VALUES (?)", [(f"Department{i}",) for i in range(1, num_departments + 1)])

def generate_courses(conn):
    c = conn.cursor()
    c.executemany("INSERT INTO Courses (CourseName, Credits, DepartmentID) VALUES (?, ?, ?)",
                [
                    ("Course1", 3, 1), ("Course2", 4, 1), ("Course3", 3, 1),
                    ("Course4", 3, 2), ("Course5", 4, 2), ("Course6", 3, 2),
                    ("Course7", 3, 3), ("Course8", 3, 3)
                ])

def generate_students(conn, num_students=100):
    c = conn.cursor()
    today = date.today()
    students = []
    for i in range(1, num_students + 1):
        first_name = f"FirstName{i}"
        last_name = f"LastName{i}"
        major_id = random.randint(1, 5)
        city_id = random.randint(1, 10)
        enrollment_date = today - timedelta(days=random.randint(365, 365 * 4))
        students.append((i, first_name, last_name, major_id, city_id, enrollment_date))
    c.executemany("INSERT INTO Students VALUES (?, ?, ?, ?, ?, ?)", students)

def generate_enrollments(conn, num_enrollments=200):
    c = conn.cursor()
    enrollments = set()  # Use a set to track unique enrollments
    students_courses = {}  # Track courses per student
    all_students = [i for i in range(1, 101)]  # List of all student IDs
    all_courses = [i for i in range(1, 9)]  # List of all course IDs

    for _ in range(num_enrollments):
        student_id = random.choice(all_students)
        available_courses = [
            course_id for course_id in all_courses if course_id not in students_courses.get(student_id, [])
        ]
        if not available_courses:
            continue  # Skip if the student has taken all courses
        course_id = random.choice(available_courses)
        grade = round(random.uniform(60, 100), 2)

        enrollments.add((student_id, course_id, grade))
        students_courses.setdefault(student_id, []).append(course_id)

    c.executemany("INSERT INTO Enrollments (StudentID, CourseID, Grade, EnrollmentDate) VALUES (?, ?, ?, ?)",
                  [
                      (student_id, course_id, grade, date.today())
                      for student_id, course_id, grade in enrollments
                  ])

def populate(conn):
    generate_cities(conn)
    generate_majors(conn)
    generate_departments(conn)
    generate_courses(conn)
    generate_students(conn)
    generate_enrollments(conn)
    conn.commit()