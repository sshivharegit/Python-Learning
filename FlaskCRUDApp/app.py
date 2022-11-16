from os import abort
from flask import Flask, request, render_template, redirect
from database import db, StudentModel

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///Students.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
 
@app.before_first_request
def create_table():
    db.create_all()

@app.route('/Students/create', methods = ['GET','POST'])
def new_data():
    if request.method == "GET":
        return render_template('studentdetails.html')
    
    if request.method == "POST":
        name= request.form['name']
        age = request.form['age']
        roll_number = request.form['roll_number']
        phone_number = request.form['phone_number']
        email = request.form['email']
        print('email', email)
        students = StudentModel(name=name, age=age, roll_number=roll_number, phone_number=phone_number, email=email)
        db.session.add(students)
        print("student", students)
        db.session.commit()
        return redirect('/Students')
    
@app.route('/Students')
def Students_data():
    print('calling student api')
    students = StudentModel.query.all()
    return render_template('studentdata.html',students = students)

@app.route('/Students/')
def RetrieveStudent(id):
    student = StudentModel.query.filter_by(id=id).first()
    if student:
        return render_template('data.html', student = student)
    return f"Student with id ={id} Doesn't exist"
 
 
@app.route('/Students/<int:id>/update',methods = ['GET','POST'])
def update(id):
    student = StudentModel.query.filter_by(id=id).first()
    if request.method == 'POST':
        if student:
            db.session.delete(student)
            db.session.commit()
            name = request.form['name']
            age = request.form['age']
            email = request.form['email']
            phone_number = request.form['phone_number']
            student = StudentModel(name=name, age=age, email=email, phone_number=phone_number)
            db.session.add(student)
            db.session.commit()
            return redirect(f'/Students/{id}')
        return f"student with roll_number = {id} Does not exist"
 
    return render_template('update.html', student = student)
 
 
@app.route('/Students/<int:id>/delete', methods=['GET','POST'])
def delete(id):
    student = StudentModel.query.filter_by(id=id).first()
    if request.method == 'POST':
        if student:
            db.session.delete(student)
            db.session.commit()
            return redirect('/Students')
        abort(404)
 
    return render_template('delete.html')

if __name__ == "__main__":
    app.run(host='localhost', debug=True)
