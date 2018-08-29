import os

from time import time

from bokeh.io import curdoc
from bokeh.layouts import layout, widgetbox
from bokeh.models import (
    Button,
    Div,
    PreText,
    Select,
    Slider,
    TextInput,
)
from jinja2 import Template
from pyspark import SparkContext, SQLContext

from pooler import unblock_with_finish

###
# Initial setup
###

DATA_DIR = os.environ.get('DATA_DIR')
APP_DIR = os.path.dirname(__file__)
DATA_FILE = os.path.join(DATA_DIR, 'clean.parquet')

sc = SparkContext("local", "Text Search")
spark = SQLContext(sc)
with open(os.path.join(APP_DIR, 'results.jinja'), 'r') as f:
    results_template = Template(f.read())

###
# Setup bokeh objects
###

column_to_look_in = Select(
    title="Column to look in",
    options=["script_url", "location", "argument_0", "value_1000"],
    value="script_url",
)
text_to_find = TextInput(title="Text to search for", value="google-analytics")
sample_frac = Slider(title="% of dataset to use", start=1, end=100, step=1, value=5)
apply_button = Button(label="Run")
save_button = Button(label="Save Results")
widgets = widgetbox(
    column_to_look_in,
    text_to_find,
    sample_frac,
    apply_button,
    save_button,
    width=300,
)
results_head = Div(text="<h2>Results</h2>")
results = PreText(text="", width=700)
spark_info = Div(text="spark info")

# Layout and add to doc
curdoc().add_root(layout([
    [widgets, [results_head, results, spark_info]]
]))

###
# Setup callbacks
###


def periodic_task():
    t = time()
    spark_info.text = f'time: {t}'


def update_results():
    result_text = results_template.render(
        count=f'{count:,}',
    )
    results.text = "\n".join([results.text, result_text])


@unblock_with_finish(update_results)
def get_new_data():
    global count
    df = spark.read.parquet(DATA_FILE)
    frac = sample_frac.value / 100
    sample = df.sample(False, frac)
    rows = sample.where(df[column_to_look_in.value].contains(text_to_find.value))
    count = rows.count()


apply_button.on_click(get_new_data)  # noqa
curdoc().add_periodic_callback(periodic_task, 1000)
