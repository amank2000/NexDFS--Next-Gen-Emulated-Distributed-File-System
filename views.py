import os
from flask import Flask, render_template, request

app = Flask(__name__)

# Route for home page
@app.route('/')
def home():
    directory = './'  # Set the initial directory
    files = os.listdir(directory)  # Get the list of files in the directory
    return render_template('index.html', directory=directory, files=files)

# Route for handling file navigation
@app.route('/navigate', methods=['POST'])
def navigate():
    directory = request.form['directory']  # Get the selected directory from the form
    files = os.listdir(directory)  # Get the list of files in the directory
    return render_template('index.html', directory=directory, files=files)

if __name__ == '__main__':
    app.run(debug=True)
