import os

from concurrent.futures import ThreadPoolExecutor
from functools import partial

from bokeh.document import without_document_lock
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
from tornado.gen import coroutine

###
# Initial setup
###

DATA_DIR = os.environ.get('DATA_DIR')
APP_DIR = os.path.dirname(__file__)
DATA_FILE = os.path.join(DATA_DIR, 'clean.parquet')
EXECUTOR = ThreadPoolExecutor(max_workers=1)

doc = curdoc()
sc = SparkContext.getOrCreate()
spark = SQLContext(sc)
st = sc.statusTracker()
with open(os.path.join(APP_DIR, 'templates', 'results.jinja'), 'r') as f:
    results_template = Template(f.read())
with open(os.path.join(APP_DIR, 'templates', 'spark.jinja'), 'r') as f:
    spark_template = Template(f.read())

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
spark_head = Div(text="""
<h4>Spark info</h4>
<p>If you see info here, but you're not running a job. Someone else is also using the server.</p>""")
spark_info = PreText(css_classes=["spark-info"], text="")
widgets = widgetbox(
    column_to_look_in,
    text_to_find,
    sample_frac,
    apply_button,
    spark_head,
    spark_info,
    width=300,
)
results_head = Div(text="<h2>Results</h2>")
results = PreText(css_classes=["results"], text="", width=700, height=500)

# Layout and add to doc
doc.add_root(layout([
    [widgets, [results_head, results]]
]))

###
# Setup callbacks
###



def periodic_task():
    active_jobs = st.getActiveJobsIds()
    active_stages = st.getActiveStageIds()
    active_stage_info = [st.getStageInfo(s) for s in active_stages]
    spark_text = spark_template.render(
        active_jobs=active_jobs,
        active_stages=active_stages,
        active_stage_info=active_stage_info,
    )
    spark_info.text = spark_text


def start_apply():
    apply_button.label = "Running"
    apply_button.disabled = True


def update_results(count):
    apply_button.label = "Run"
    apply_button.disabled = False
    result_text = results_template.render(
        count=f'{count:,}',
    )
    results.text = "\n".join([results.text, result_text])


def get_count(df):
    return df.count()


@coroutine
@without_document_lock
def get_new_data():
    doc.add_next_tick_callback(start_apply)
    df = spark.read.parquet(DATA_FILE)
    frac = sample_frac.value / 100
    sample = df.sample(False, frac)
    rows = sample.where(df[column_to_look_in.value].contains(text_to_find.value))
    count = yield EXECUTOR.submit(get_count, rows)
    doc.add_next_tick_callback(partial(update_results, count))


apply_button.on_click(get_new_data)  # noqa
doc.add_periodic_callback(periodic_task, 1000)
