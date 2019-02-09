from flask import Flask, Markup, render_template

app = Flask(__name__)

# connecting to mysql

@app.route('/')
def bar1():
    bar_labels=labels
    bar_values=values
    return render_template('bar_chart.html', title='Model Quality Assessment', max=17000, labels=bar_labels, values=bar_values)

@app.route('/bar_chart')
def bar2():
    bar_labels=labels
    bar_values=values
    return render_template('bar_chart.html', title='Model Quality Assessment', max=17000, labels=bar_labels, values=bar_values)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
