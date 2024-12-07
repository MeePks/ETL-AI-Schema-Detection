from flask import Flask,render_template,request,jsonify

app = Flask(__name__)



@app.route('/')
def home():
    return render_template('index.html', message="Hello, Flask with Templates!")


@app.route('/about')
def about():
    return "This is the about page."

@app.route('/user/<name>')
def user(name):
    return f"Hello, {name}!"

@app.route('/form')
def form():
    return render_template('form.html')

@app.route('/submit', methods=['POST'])
def submit():
    username = request.form.get('username')
    return f"Hello, {username}!"

@app.route('/api/data', methods=['GET'])
def get_data():
    data = {"name": "John", "age": 30, "job": "developer"}
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
