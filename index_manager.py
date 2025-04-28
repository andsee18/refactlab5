import sqlite3
def create_idx(conn):
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("CREATE INDEX IF NOT EXISTS idx_students_cityid ON Students (CityID)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_courses_departmentid ON Courses (DepartmentID)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_studentid ON Enrollments (StudentID)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_enrollments_courseid ON Enrollments (CourseID)")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Index Error: {e}")

def drop_idx(conn):
    if not conn: return
    try:
        c = conn.cursor()
        c.execute("DROP INDEX IF EXISTS idx_enrollments_enrollmentdate")
        conn.commit()
    except sqlite3.Error as e:
        print(f"Index Error: {e}")

def explain(conn, query, params=None):
    if not conn: return None
    try:
        c = conn.cursor()
        c.execute(f"EXPLAIN QUERY PLAN {query}", params or ())
        return c.fetchall()
    except sqlite3.Error as e:
        print(f"Explain Error: {e}")
        return None