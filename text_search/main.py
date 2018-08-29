import os

from time import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from bokeh.document import without_document_lock
from bokeh.io import curdoc
from bokeh.layouts import layout, widgetbox
from bokeh.models import (
    Button,
    ColumnDataSource,
    Div,
    PreText,
    Select,
    Slider,
    TextInput,
)
from bokeh.plotting import figure
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
progress_source = ColumnDataSource({'complete': [0]})
progress_bar = figure(
    tools='', height=40, width=300, outline_line_color=None, min_border=10
)
ready_color = 'mediumseagreen'
process_color = 'lightblue'
progress_bar_opts = dict(height=1, y=0, line_color=None)
progress_bar_background = progress_bar.hbar(right=[1], color=ready_color, alpha=0.3, **progress_bar_opts)
progress_bar.hbar(right='complete', color=process_color, source=progress_source, **progress_bar_opts)
progress_bar.grid.visible = False
progress_bar.axis.visible = False
progress_bar.toolbar_location = None
widgets = widgetbox(
    column_to_look_in,
    text_to_find,
    sample_frac,
    apply_button,
    width=300,
)
results_head = Div(text="<h2>Results</h2>")
results = PreText(css_classes=["results"], text="", width=700, height=500)

# Layout and add to doc
doc.add_root(layout([[
    [widgets, progress_bar],
    [results_head, results]
]]))

###
# Setup callbacks
###

st = sc.statusTracker()


def poll_progress_bar():
    active_stage_ids = st.getActiveStageIds()
    if len(active_stage_ids) == 0:
        progress_bar_background.glyph.fill_color = ready_color
        if progress_source.data['complete'][0] != 0:
            progress_source.data = {'complete': [0]}
        return
    active_stage_id = active_stage_ids[0]
    stage_info = st.getStageInfo(active_stage_id)
    num_tasks = stage_info.numTasks
    num_completed_tasks = stage_info.numCompletedTasks
    progress_bar_background.glyph.fill_color = process_color
    progress_source.data = {'complete': [num_completed_tasks / num_tasks]}


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
doc.add_periodic_callback(poll_progress_bar, 200)
