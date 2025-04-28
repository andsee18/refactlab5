# queries.py
def get_students_from_city_no_index(conn, city_name):
    c = conn.cursor()
    c.execute("SELECT s.* FROM Students s JOIN Cities c ON s.CityID = c.CityID WHERE c.CityName = ?", (city_name,))
    return c.fetchall()

def get_students_from_city_with_index(conn, city_name):
    c = conn.cursor()
    c.execute("SELECT s.StudentID, s.FirstName, s.LastName, m.MajorName, c.CityName, s.EnrollmentDate FROM Students s" \
             " JOIN Cities c ON s.CityID = c.CityID JOIN Majors m ON s.MajorID = m.MajorID WHERE c.CityName = ?", (city_name,))
    return c.fetchall()

def get_courses_in_department_inefficient(conn, dept_name):
    c = conn.cursor()
    c.execute("SELECT c.* FROM Courses c WHERE c.DepartmentID IN (SELECT DepartmentID FROM Departments WHERE DepartmentName = ?)", (dept_name,))
    return c.fetchall()

def get_courses_in_department_optimized(conn, dept_name):
    c = conn.cursor()
    c.execute("SELECT c.CourseID, c.CourseName, c.Credits, d.DepartmentName FROM Courses c JOIN Departments d ON c.DepartmentID = d.DepartmentID WHERE d.DepartmentName = ?", (dept_name,))
    return c.fetchall()

def get_student_enrollments_with_details_complex_inefficient(conn):
    c = conn.cursor()
    c.execute("SELECT s.*, m.*, c.*, e.*, d.*, city.* FROM Students s JOIN Majors m ON s.MajorID = m.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID JOIN Courses c ON e.CourseID" \
    " = c.CourseID JOIN Departments d ON c.DepartmentID = d.DepartmentID JOIN Cities city ON s.CityID = city.CityID")
    return c.fetchall()

def get_student_enrollments_with_details_complex_efficient(conn):
    c = conn.cursor()
    c.execute("SELECT s.StudentID, s.FirstName, s.LastName, m.MajorName, c.CourseName, c.Credits, d.DepartmentName, e.Grade, e.EnrollmentDate, city.CityName FROM Students s " \
    "JOIN Majors m ON s.MajorID = m.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID JOIN Courses c ON e.CourseID = c.CourseID JOIN Departments d ON c.DepartmentID = d.DepartmentID" \
    " JOIN Cities city ON s.CityID = city.CityID")
    return c.fetchall()

def get_average_grade_per_major_inefficient(conn):
    c = conn.cursor()
    c.execute("SELECT m.MajorName, AVG(e.Grade) FROM Majors m JOIN Students s ON m.MajorID = s.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID GROUP BY m.MajorName")
    return c.fetchall()

def get_average_grade_per_major_optimized(conn):
    c = conn.cursor()
    c.execute("""
        SELECT m.MajorName, AVG(e.Grade) 
        FROM Majors m 
        JOIN Students s ON m.MajorID = s.MajorID 
        JOIN Enrollments e ON s.StudentID = e.StudentID 
        GROUP BY m.MajorName
    """)
    return c.fetchall()