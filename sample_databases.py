"""
╔══════════════════════════════════════════════════════════════╗
║  sample_databases.py  —  Creates 3 realistic demo databases  ║
╚══════════════════════════════════════════════════════════════╝

Creates three complete SQLite databases with realistic Indian data:
  1. College Database    — students, departments, professors, courses, enrollments
  2. E-Commerce Database — customers, products, categories, orders, order_items
  3. Hospital Database   — patients, doctors, appointments, prescriptions, medicines

WHY GOOD SAMPLE DATA MATTERS:
  - Evaluators can query immediately without uploading anything
  - Realistic names and values make demos more convincing
  - Multiple related tables let you demonstrate JOIN queries
  - Enough rows (100+ per table) to show aggregation results

RUN THIS ONCE:
  python sample_databases.py
  → Creates databases/college.db, databases/ecommerce.db, databases/hospital.db
"""

import os
import sqlite3
import random
from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────
#  SHARED DATA POOLS
# ─────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Arjun", "Priya", "Rahul", "Ananya", "Vikram", "Sneha", "Aditya", "Kavya",
    "Rohan", "Deepika", "Karan", "Pooja", "Nikhil", "Shruti", "Amit", "Meera",
    "Suresh", "Lakshmi", "Rajesh", "Divya", "Sanjay", "Ritu", "Manish", "Nisha",
    "Prakash", "Sunita", "Vinay", "Geeta", "Ashish", "Poonam", "Ravi", "Sona",
    "Gaurav", "Aarti", "Vivek", "Reena", "Deepak", "Kanika", "Ajay", "Swati",
    "Harish", "Preeti", "Nitin", "Mala", "Ramesh", "Saroj", "Sunil", "Anjali",
    "Mahesh", "Rekha", "Tarun", "Smita", "Pavan", "Neha", "Chetan", "Monika",
    "Yogesh", "Suman", "Dinesh", "Jyoti", "Arun", "Bharti", "Sachin", "Radha",
]

LAST_NAMES = [
    "Sharma", "Patel", "Gupta", "Singh", "Kumar", "Mehta", "Reddy", "Nair",
    "Joshi", "Verma", "Mishra", "Rao", "Iyer", "Pillai", "Bhat", "Hegde",
    "Chaudhary", "Pandey", "Tiwari", "Srivastava", "Yadav", "Shah", "Desai", "More",
    "Kulkarni", "Patil", "Gaikwad", "Shinde", "Pawar", "Sawant", "Jadhav", "Mane",
]

CITIES = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
          "Pune", "Ahmedabad", "Jaipur", "Kolkata", "Surat",
          "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane"]

STATES = {
    "Mumbai": "Maharashtra", "Delhi": "Delhi", "Bangalore": "Karnataka",
    "Hyderabad": "Telangana", "Chennai": "Tamil Nadu", "Pune": "Maharashtra",
    "Ahmedabad": "Gujarat", "Jaipur": "Rajasthan", "Kolkata": "West Bengal",
    "Surat": "Gujarat", "Lucknow": "Uttar Pradesh", "Kanpur": "Uttar Pradesh",
    "Nagpur": "Maharashtra", "Indore": "Madhya Pradesh", "Thane": "Maharashtra",
}


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_email(name: str, domain: str) -> str:
    return name.lower().replace(" ", ".") + str(random.randint(1, 99)) + f"@{domain}"


def random_date(start_year: int = 2020, end_year: int = 2024) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")


def random_phone() -> str:
    return f"+91 {random.randint(7000000000, 9999999999)}"


# ─────────────────────────────────────────────────────────
#  COLLEGE DATABASE
# ─────────────────────────────────────────────────────────

def create_college_db():
    """
    Creates college.db with 5 related tables:
      departments → professors, students
      courses (linked to professors)
      enrollments (students ↔ courses, with grades)
    """
    os.makedirs("databases", exist_ok=True)
    conn = sqlite3.connect("databases/college.db")
    cur  = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS enrollments;
        DROP TABLE IF EXISTS courses;
        DROP TABLE IF EXISTS students;
        DROP TABLE IF EXISTS professors;
        DROP TABLE IF EXISTS departments;

        CREATE TABLE departments (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            hod  TEXT,
            established_year INTEGER
        );

        CREATE TABLE professors (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            email    TEXT UNIQUE,
            dept_id  INTEGER REFERENCES departments(id),
            designation TEXT,
            experience_years INTEGER,
            salary   REAL
        );

        CREATE TABLE students (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            email    TEXT UNIQUE,
            dept_id  INTEGER REFERENCES departments(id),
            cgpa     REAL CHECK(cgpa >= 0.0 AND cgpa <= 10.0),
            year     INTEGER CHECK(year BETWEEN 1 AND 4),
            city     TEXT,
            phone    TEXT,
            dob      TEXT
        );

        CREATE TABLE courses (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            code         TEXT UNIQUE,
            dept_id      INTEGER REFERENCES departments(id),
            professor_id INTEGER REFERENCES professors(id),
            credits      INTEGER DEFAULT 3,
            semester     INTEGER CHECK(semester BETWEEN 1 AND 8)
        );

        CREATE TABLE enrollments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER REFERENCES students(id),
            course_id  INTEGER REFERENCES courses(id),
            grade      TEXT,
            marks      REAL,
            semester   TEXT,
            year       INTEGER
        );
    """)

    DEPTS = [
        ("Computer Science", "Dr. Rajesh Kumar", 1985),
        ("Electrical Engineering", "Dr. Meera Sharma", 1978),
        ("Mechanical Engineering", "Dr. Suresh Patel", 1975),
        ("Civil Engineering", "Dr. Anjali Singh", 1970),
        ("Information Technology", "Dr. Vivek Gupta", 1998),
        ("Electronics", "Dr. Priya Nair", 1982),
    ]
    cur.executemany("INSERT INTO departments (name, hod, established_year) VALUES (?,?,?)", DEPTS)
    dept_ids = list(range(1, len(DEPTS) + 1))

    DESIGNATIONS = ["Assistant Professor", "Associate Professor", "Professor", "Senior Professor"]
    for i in range(30):
        name = random_name()
        cur.execute(
            "INSERT INTO professors (name,email,dept_id,designation,experience_years,salary) VALUES (?,?,?,?,?,?)",
            (name, random_email(name, "college.edu"), random.choice(dept_ids),
             random.choice(DESIGNATIONS), random.randint(2, 35),
             round(random.uniform(50000, 180000), 2))
        )

    for i in range(150):
        name = random_name()
        cur.execute(
            "INSERT INTO students (name,email,dept_id,cgpa,year,city,phone,dob) VALUES (?,?,?,?,?,?,?,?)",
            (name, random_email(name, "student.edu"), random.choice(dept_ids),
             round(random.uniform(5.5, 10.0), 2), random.randint(1, 4),
             random.choice(CITIES), random_phone(),
             random_date(1999, 2005))
        )

    COURSE_NAMES = [
        ("Data Structures", "CS101"), ("Algorithms", "CS102"),
        ("Database Management", "CS201"), ("Operating Systems", "CS202"),
        ("Machine Learning", "CS301"), ("Computer Networks", "CS302"),
        ("Web Development", "CS401"), ("Artificial Intelligence", "CS402"),
        ("Circuit Theory", "EE101"), ("Digital Electronics", "EE102"),
        ("Power Systems", "EE201"), ("Control Systems", "EE202"),
        ("Thermodynamics", "ME101"), ("Fluid Mechanics", "ME102"),
        ("Structural Analysis", "CE101"), ("Soil Mechanics", "CE102"),
        ("Python Programming", "IT101"), ("Cloud Computing", "IT201"),
    ]
    for name, code in COURSE_NAMES:
        dept = 1 if code.startswith("CS") else (2 if code.startswith("EE") else
               3 if code.startswith("ME") else 4 if code.startswith("CE") else 5)
        cur.execute(
            "INSERT INTO courses (name,code,dept_id,professor_id,credits,semester) VALUES (?,?,?,?,?,?)",
            (name, code, dept, random.randint(1, 30), random.randint(2, 4), random.randint(1, 8))
        )

    GRADES = ["O", "A+", "A", "B+", "B", "C", "D", "F"]
    GRADE_MARKS = {"O": (90,100), "A+": (80,89), "A": (70,79), "B+": (60,69),
                   "B": (50,59), "C": (40,49), "D": (35,39), "F": (0,34)}
    for student_id in range(1, 151):
        for course_id in random.sample(range(1, len(COURSE_NAMES)+1), random.randint(3, 6)):
            grade = random.choice(GRADES)
            lo, hi = GRADE_MARKS[grade]
            cur.execute(
                "INSERT INTO enrollments (student_id,course_id,grade,marks,semester,year) VALUES (?,?,?,?,?,?)",
                (student_id, course_id, grade, round(random.uniform(lo, hi), 1),
                 f"Sem {random.randint(1,8)}", random.randint(2021, 2024))
            )

    conn.commit()
    conn.close()
    print("✅  College database created  → databases/college.db")


# ─────────────────────────────────────────────────────────
#  E-COMMERCE DATABASE
# ─────────────────────────────────────────────────────────

def create_ecommerce_db():
    """
    Creates ecommerce.db with 5 related tables:
      categories → products
      customers → orders → order_items (linking products)
    """
    os.makedirs("databases", exist_ok=True)
    conn = sqlite3.connect("databases/ecommerce.db")
    cur  = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS categories;
        DROP TABLE IF EXISTS customers;

        CREATE TABLE categories (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        );

        CREATE TABLE products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            category_id INTEGER REFERENCES categories(id),
            price       REAL NOT NULL,
            stock       INTEGER DEFAULT 0,
            rating      REAL CHECK(rating BETWEEN 1 AND 5),
            brand       TEXT,
            sku         TEXT UNIQUE
        );

        CREATE TABLE customers (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL,
            email    TEXT UNIQUE,
            phone    TEXT,
            city     TEXT,
            state    TEXT,
            joined_date TEXT
        );

        CREATE TABLE orders (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id     INTEGER REFERENCES customers(id),
            order_date      TEXT NOT NULL,
            status          TEXT CHECK(status IN ('Pending','Processing','Shipped','Delivered','Cancelled')),
            total_amount    REAL,
            payment_method  TEXT,
            delivery_date   TEXT
        );

        CREATE TABLE order_items (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id   INTEGER REFERENCES orders(id),
            product_id INTEGER REFERENCES products(id),
            quantity   INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
    """)

    CATEGORIES = [
        ("Electronics",    "Phones, laptops, TVs, accessories"),
        ("Clothing",       "Men and women fashion apparel"),
        ("Home & Kitchen", "Appliances and kitchenware"),
        ("Books",          "Educational and fiction books"),
        ("Sports",         "Equipment and fitness gear"),
        ("Beauty",         "Skincare and cosmetics"),
        ("Toys",           "Children and adult toys"),
        ("Groceries",      "Food and daily essentials"),
    ]
    cur.executemany("INSERT INTO categories (name,description) VALUES (?,?)", CATEGORIES)

    PRODUCTS = [
        ("iPhone 15", 1, 79999, 45, 4.8, "Apple"),
        ("Samsung Galaxy S24", 1, 64999, 78, 4.7, "Samsung"),
        ("OnePlus 12", 1, 49999, 120, 4.6, "OnePlus"),
        ("MacBook Air M3", 1, 119999, 30, 4.9, "Apple"),
        ("Dell Inspiron 15", 1, 55000, 60, 4.4, "Dell"),
        ("Sony WH-1000XM5", 1, 29990, 200, 4.8, "Sony"),
        ("Men's T-Shirt", 2, 499, 500, 4.2, "H&M"),
        ("Women's Kurta", 2, 799, 400, 4.5, "W"),
        ("Running Shoes", 2, 2499, 300, 4.3, "Nike"),
        ("Jeans Slim Fit", 2, 1299, 350, 4.1, "Levi's"),
        ("Microwave Oven", 3, 7499, 80, 4.3, "LG"),
        ("Air Fryer", 3, 3999, 150, 4.6, "Philips"),
        ("Pressure Cooker", 3, 1299, 250, 4.5, "Prestige"),
        ("Water Purifier", 3, 14999, 55, 4.4, "Kent"),
        ("Data Structures Book", 4, 599, 200, 4.7, "Pearson"),
        ("Python Cookbook", 4, 799, 180, 4.6, "O'Reilly"),
        ("Cricket Bat", 5, 1999, 120, 4.3, "MRF"),
        ("Yoga Mat", 5, 699, 300, 4.5, "Lifelong"),
        ("Moisturiser SPF50", 6, 449, 400, 4.2, "Neutrogena"),
        ("LEGO Set", 7, 3499, 90, 4.8, "LEGO"),
        ("Basmati Rice 5kg", 8, 449, 500, 4.4, "India Gate"),
        ("Olive Oil 1L", 8, 599, 300, 4.3, "Figaro"),
    ]
    for name, cat_id, price, stock, rating, brand in PRODUCTS:
        sku = f"SKU-{random.randint(100000, 999999)}"
        cur.execute(
            "INSERT INTO products (name,category_id,price,stock,rating,brand,sku) VALUES (?,?,?,?,?,?,?)",
            (name, cat_id, price, stock, rating, brand, sku)
        )

    for i in range(200):
        name = random_name()
        city = random.choice(CITIES)
        cur.execute(
            "INSERT INTO customers (name,email,phone,city,state,joined_date) VALUES (?,?,?,?,?,?)",
            (name, random_email(name, "gmail.com"), random_phone(),
             city, STATES[city], random_date(2020, 2024))
        )

    STATUSES     = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]
    STATUS_WGTS  = [0.05, 0.10, 0.15, 0.60, 0.10]
    PAYMENTS     = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash on Delivery", "Wallet"]

    for order_id in range(1, 501):
        cust_id    = random.randint(1, 200)
        order_date = random_date(2023, 2024)
        status     = random.choices(STATUSES, STATUS_WGTS)[0]
        payment    = random.choice(PAYMENTS)
        cur.execute(
            "INSERT INTO orders (customer_id,order_date,status,payment_method) VALUES (?,?,?,?)",
            (cust_id, order_date, status, payment)
        )
        total = 0.0
        for _ in range(random.randint(1, 4)):
            prod_id  = random.randint(1, len(PRODUCTS))
            qty      = random.randint(1, 3)
            cur.execute("SELECT price FROM products WHERE id=?", (prod_id,))
            price    = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (?,?,?,?)",
                (order_id, prod_id, qty, price)
            )
            total += price * qty
        cur.execute("UPDATE orders SET total_amount=? WHERE id=?", (round(total, 2), order_id))

    conn.commit()
    conn.close()
    print("✅  E-Commerce database created  → databases/ecommerce.db")


# ─────────────────────────────────────────────────────────
#  HOSPITAL DATABASE
# ─────────────────────────────────────────────────────────

def create_hospital_db():
    """
    Creates hospital.db with 5 related tables:
      departments → doctors
      patients → appointments (linking doctors)
      prescriptions → medicines (through prescriptions)
    """
    os.makedirs("databases", exist_ok=True)
    conn = sqlite3.connect("databases/hospital.db")
    cur  = conn.cursor()

    cur.executescript("""
        DROP TABLE IF EXISTS prescriptions;
        DROP TABLE IF EXISTS appointments;
        DROP TABLE IF EXISTS medicines;
        DROP TABLE IF EXISTS patients;
        DROP TABLE IF EXISTS doctors;
        DROP TABLE IF EXISTS departments;

        CREATE TABLE departments (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            head TEXT,
            floor INTEGER
        );

        CREATE TABLE doctors (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL,
            dept_id    INTEGER REFERENCES departments(id),
            specialisation TEXT,
            experience_years INTEGER,
            consultation_fee REAL,
            email      TEXT UNIQUE,
            phone      TEXT
        );

        CREATE TABLE patients (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            age          INTEGER,
            gender       TEXT CHECK(gender IN ('Male','Female','Other')),
            blood_group  TEXT,
            city         TEXT,
            phone        TEXT,
            admitted_date TEXT,
            discharge_date TEXT,
            diagnosis    TEXT
        );

        CREATE TABLE medicines (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL UNIQUE,
            category TEXT,
            price    REAL
        );

        CREATE TABLE appointments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER REFERENCES patients(id),
            doctor_id  INTEGER REFERENCES doctors(id),
            appt_date  TEXT NOT NULL,
            appt_time  TEXT,
            status     TEXT CHECK(status IN ('Scheduled','Completed','Cancelled','No-Show')),
            notes      TEXT
        );

        CREATE TABLE prescriptions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            appointment_id INTEGER REFERENCES appointments(id),
            medicine_id    INTEGER REFERENCES medicines(id),
            dosage         TEXT,
            duration_days  INTEGER
        );
    """)

    HOSPITAL_DEPTS = [
        ("Cardiology",        "Dr. Rajesh Mehta",   4),
        ("Neurology",         "Dr. Priya Sharma",   3),
        ("Orthopaedics",      "Dr. Suresh Patel",   2),
        ("Paediatrics",       "Dr. Ananya Singh",   1),
        ("Oncology",          "Dr. Vikram Gupta",   5),
        ("Gynaecology",       "Dr. Sneha Reddy",    2),
        ("General Medicine",  "Dr. Aditya Kumar",   1),
        ("Emergency",         "Dr. Kavya Nair",     0),
    ]
    cur.executemany("INSERT INTO departments (name,head,floor) VALUES (?,?,?)", HOSPITAL_DEPTS)
    dept_ids = list(range(1, len(HOSPITAL_DEPTS) + 1))

    SPECIALISATIONS = {
        1: "Heart Specialist", 2: "Brain & Spine", 3: "Bone & Joint",
        4: "Child Health", 5: "Cancer Treatment", 6: "Women Health",
        7: "General Practice", 8: "Emergency Care",
    }
    for i in range(40):
        name    = "Dr. " + random_name()
        dept_id = random.choice(dept_ids)
        cur.execute(
            "INSERT INTO doctors (name,dept_id,specialisation,experience_years,consultation_fee,email,phone) VALUES (?,?,?,?,?,?,?)",
            (name, dept_id, SPECIALISATIONS[dept_id],
             random.randint(3, 30),
             round(random.choice([300, 500, 700, 1000, 1500, 2000]), 2),
             random_email(name, "hospital.in"), random_phone())
        )

    DIAGNOSES = [
        "Hypertension", "Type 2 Diabetes", "Fever", "Fracture",
        "Appendicitis", "Migraine", "Asthma", "Pneumonia",
        "Urinary Tract Infection", "Anaemia", "Thyroid Disorder",
        "Dengue", "Malaria", "COVID-19", "Arthritis",
        "Kidney Stone", "Liver Disease", "Depression", "Obesity",
    ]
    BLOOD_GROUPS = ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
    GENDERS      = ["Male", "Female", "Other"]

    for i in range(200):
        admitted = random_date(2023, 2024)
        discharged = None
        if random.random() > 0.2:
            discharged = (datetime.strptime(admitted, "%Y-%m-%d") +
                          timedelta(days=random.randint(1, 15))).strftime("%Y-%m-%d")
        cur.execute(
            "INSERT INTO patients (name,age,gender,blood_group,city,phone,admitted_date,discharge_date,diagnosis) VALUES (?,?,?,?,?,?,?,?,?)",
            (random_name(), random.randint(1, 85),
             random.choices(GENDERS, [0.50, 0.48, 0.02])[0],
             random.choice(BLOOD_GROUPS), random.choice(CITIES),
             random_phone(), admitted, discharged,
             random.choice(DIAGNOSES))
        )

    MEDICINES = [
        ("Paracetamol",   "Analgesic",     15),
        ("Amoxicillin",   "Antibiotic",    45),
        ("Metformin",     "Antidiabetic",  30),
        ("Amlodipine",    "Antihypertensive", 25),
        ("Omeprazole",    "Antacid",       40),
        ("Cetirizine",    "Antihistamine", 20),
        ("Ibuprofen",     "NSAID",         18),
        ("Azithromycin",  "Antibiotic",    80),
        ("Atorvastatin",  "Statin",        55),
        ("Levothyroxine", "Thyroid",       35),
        ("Pantoprazole",  "Antacid",       38),
        ("Aspirin",       "Antiplatelet",  12),
        ("Lisinopril",    "ACE Inhibitor", 42),
        ("Insulin",       "Antidiabetic",  320),
        ("Salbutamol",    "Bronchodilator",28),
    ]
    cur.executemany("INSERT INTO medicines (name,category,price) VALUES (?,?,?)", MEDICINES)

    STATUS_CHOICES = ["Scheduled", "Completed", "Cancelled", "No-Show"]
    STATUS_WGTS    = [0.15, 0.65, 0.12, 0.08]

    for appt_id in range(1, 401):
        patient_id = random.randint(1, 200)
        doctor_id  = random.randint(1, 40)
        status     = random.choices(STATUS_CHOICES, STATUS_WGTS)[0]
        cur.execute(
            "INSERT INTO appointments (patient_id,doctor_id,appt_date,appt_time,status) VALUES (?,?,?,?,?)",
            (patient_id, doctor_id, random_date(2023, 2024),
             f"{random.randint(9,17):02d}:{random.choice(['00','30'])}",
             status)
        )
        if status == "Completed":
            for _ in range(random.randint(1, 3)):
                cur.execute(
                    "INSERT INTO prescriptions (appointment_id,medicine_id,dosage,duration_days) VALUES (?,?,?,?)",
                    (appt_id, random.randint(1, len(MEDICINES)),
                     random.choice(["Once daily", "Twice daily", "Three times daily", "As needed"]),
                     random.choice([3, 5, 7, 10, 14, 30]))
                )

    conn.commit()
    conn.close()
    print("✅  Hospital database created  → databases/hospital.db")


# ─────────────────────────────────────────────────────────
#  SETUP ALL DATABASES (called by server.py on startup)
# ─────────────────────────────────────────────────────────

def setup_all_databases():
    """Create all three sample databases. Safe to call multiple times."""
    print("📦 Setting up sample databases...")
    create_college_db()
    create_ecommerce_db()
    create_hospital_db()
    print("🎉 All sample databases ready!\n")


if __name__ == "__main__":
    setup_all_databases()
