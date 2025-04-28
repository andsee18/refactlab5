# main.py
import time
import sqlite3
import os
from db_utils import db_connect, db_close, create_tables
from data_generator import populate
import queries
from index_manager import create_idx, drop_idx, explain

DB_FILE = "university_complex.db"

def fmt_time(s):
    ms = s * 1000
    if ms < 1: return f"{s:.4f}s"
    return f"{ms:.4f}ms" if ms < 1000 else f"{s:.4f}s"

def plan(c, q, p=None):
    if not c: return "NoConn"
    expl = explain(c, q, p)
    if not expl: return "NoPlan"
    smpl = []
    for r in expl:
        op = r[3]
        if "SCAN TABLE" in op: smpl.append("Scan")
        elif "USING INDEX" in op: smpl.append("Idx")
        elif "PRIMARY KEY" in op: smpl.append("PK")
        elif "JOIN" in op: smpl.append("Join")
        elif "SUBQUERY" in op: smpl.append("Sub")
        elif "GROUP BY" in op: smpl.append("Grp")
        else: smpl.append(op)
    return ", ".join(smpl)

def run(c):
    if not c: return

    tests = [
        {"n": "City", "ineff": queries.get_students_from_city_no_index,
         "eff": queries.get_students_from_city_with_index, "p": ("City5",),
         "iq": "SELECT s.* FROM Students s JOIN Cities c ON s.CityID = c.CityID WHERE c.CityName = ?",
         "eq": "SELECT s.StudentID, s.FirstName, s.LastName, m.MajorName, c.CityName, s.EnrollmentDate FROM Students s JOIN Cities c ON s.CityID = c.CityID" \
          " JOIN Majors m ON s.MajorID = m.MajorID WHERE c.CityName = ?"},
        {"n": "Courses", "ineff": queries.get_courses_in_department_inefficient,
         "eff": queries.get_courses_in_department_optimized, "p": ("Department1",),
         "iq": "SELECT c.* FROM Courses c WHERE c.DepartmentID IN (SELECT DepartmentID FROM Departments WHERE DepartmentName = ?)",
         "eq": "SELECT c.CourseID, c.CourseName, c.Credits, d.DepartmentName FROM Courses c JOIN Departments d ON c.DepartmentID = d.DepartmentID WHERE d.DepartmentName = ?"},
        {"n": "Enroll", "ineff": queries.get_student_enrollments_with_details_complex_inefficient,
         "eff": queries.get_student_enrollments_with_details_complex_efficient, "p": None,
         "iq": "SELECT s.*, m.*, c.*, e.*, d.*, city.* FROM Students s JOIN Majors m ON s.MajorID = m.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID JOIN Courses c ON e.CourseID =" \
         " c.CourseID JOIN Departments d ON c.DepartmentID = d.DepartmentID JOIN Cities city ON s.CityID = city.CityID",
         "eq": "SELECT s.StudentID, s.FirstName, s.LastName, m.MajorName, c.CourseName, c.Credits, d.DepartmentName, e.Grade, e.EnrollmentDate, city.CityName FROM Students s JOIN Majors m ON s.MajorID =" \
         " m.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID JOIN Courses c ON e.CourseID = c.CourseID JOIN Departments d ON c.DepartmentID = d.DepartmentID JOIN Cities city ON s.CityID = city.CityID"},
        {"n": "AvgGrade", "ineff": queries.get_average_grade_per_major_inefficient,
         "eff": queries.get_average_grade_per_major_optimized, "p": None,
         "iq": "SELECT m.MajorName, AVG(e.Grade) FROM Majors m JOIN Students s ON m.MajorID = s.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID GROUP BY m.MajorName",
         "eq": "SELECT m.MajorName, AVG(e.Grade) FROM Majors m ON s.MajorID = s.MajorID JOIN Enrollments e ON s.StudentID = e.StudentID GROUP BY m.MajorName"}
    ]

    print("--- Tests ---")

    for t in tests:
        print(f"\n--- {t['n']} ---")
        start = time.time()
        ineff_res = t['ineff'](c, *t['p']) if t['p'] else t['ineff'](c)
        time_ineff = time.time() - start
        print(f"Ineff: {fmt_time(time_ineff)}, Found: {len(ineff_res)}")

        start = time.time()
        eff_res = t['eff'](c, *t['p']) if t['p'] else t['eff'](c)
        time_eff = time.time() - start
        print(f"Eff: {fmt_time(time_eff)}, Found: {len(eff_res)}")

        print(f"Plan (Ineff): {plan(c, t['iq'], t['p'])}")
        print(f"Plan (Eff): {plan(c, t['eq'], t['p'])}")

def main():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    c = db_connect()
    if not c: return

    create_tables(c)  
    populate(c)      
    create_idx(c)
    run(c)
    drop_idx(c)
    db_close(c)

if __name__ == "__main__":
    main()