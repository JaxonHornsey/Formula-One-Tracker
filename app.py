from flask import Flask, render_template


app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, world!'


import csv

with open('driver_standings.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader]


@app.route('/points')
def points():
    with open('driver_standings.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]
    return render_template('points.html', data=data)


if __name__ == '__main__':
    app.run()

