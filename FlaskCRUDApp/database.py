from flask_sqlalchemy import SQLAlchemy
 
db = SQLAlchemy()
class StudentModel(db.Model):
    __tablename__ = "table"
 
    id = db.Column(db.Integer, primary_key=True)
    phone_number = db.Column(db.Integer(),unique = True)
    name = db.Column(db.String())
    roll_number = db.Column(db.Integer())
    age = db.Column(db.Integer())
    email= db.Column(db.String(80))
 
    def __init__(self,phone_number, name, roll_number, age, email):
        self.phone_number = phone_number
        self.name = name
        self.roll_number = roll_number
        self.age = age
        self.email = email
 
    def __repr__(self):
        return f"{self.name}:{self.roll_number}:{self.email}:{self.age}:{self.phone_number}"