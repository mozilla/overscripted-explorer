import os

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from urllib.parse import urlparse


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
from pyspark.sql.functions import udf
from tornado.gen import coroutine




###
# Initial setup
###

DATA_DIR = os.environ.get('DATA_DIR')
APP_DIR = os.path.dirname(__file__)
DATA_FILE = os.path.join(DATA_DIR, 'clean.parquet')
EXECUTOR = ThreadPoolExecutor(max_workers=4)

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


def update_results(
    total_count,
    filtered_count,
    # script_url_n,
    # script_url_se,
    # location_n,
    # location_se,
    script_url_nl_n,
    script_url_nl_se,
    location_nl_n,
    location_nl_se
):
    pr = lambda x: f'{x:,}'
    pc = lambda x: f'{x:.2%}'

    result_text = results_template.render(
        column=column_to_look_in.value,
        text=text_to_find.value,
        sample_size=sample_frac.value,
        sample_n_rows=pr(total_count),
        filtered_n_rows=pr(filtered_count),
        percent_row=pc(filtered_count / total_count),
        # script_url_n=pr(script_url_n),
        # script_url_se=pr(script_url_se),
        # script_url_pc=pc(script_url_se / script_url_n),
        # location_n=pr(location_n),
        # location_se=pr(location_se),
        # location_pc=pc(location_se / location_n),
        script_url_nl_n=pr(script_url_nl_n),
        script_url_nl_se=pr(script_url_nl_se),
        script_url_nl_pc=pc(script_url_nl_se / script_url_nl_n),
        location_nl_n=pr(location_nl_n),
        location_nl_se=pr(location_nl_se),
        location_nl_pc=pc(location_nl_se / location_nl_n),
    )
    results.text = "\n".join([results.text, result_text])
    apply_button.label = "Run"
    apply_button.disabled = False


def do_spark_computation(text_value):
    def get_netloc(x):
        p = urlparse(x)
        return f'{p.netloc}'
    get_netloc_udf = udf(get_netloc)

    df = spark.read.parquet(DATA_FILE)
    frac = sample_frac.value / 100
    sample = df.sample(False, frac)
    sample = sample.withColumn(
        'script_url_nl', get_netloc_udf(sample.script_url)
    ).withColumn(
        'location_nl', get_netloc_udf(sample.location)
    )
    filtered = sample.where(df[column_to_look_in.value].contains(text_value))
    total_count = sample.count()
    filtered_count = filtered.count()
    # script_url_n = sample.select('script_url').distinct().count()
    # script_url_se = filtered.select('script_url').distinct().count()
    # location_n = sample.select('location').distinct().count()
    # location_se = filtered.select('location').distinct().count()
    script_url_nl_n = sample.select('script_url_nl').distinct().count()
    script_url_nl_se = filtered.select('script_url_nl').distinct().count()
    location_nl_n = sample.select('location_nl').distinct().count()
    location_nl_se = filtered.select('location_nl').distinct().count()
    return (total_count, filtered_count, script_url_nl_n, script_url_nl_se, location_nl_n, location_nl_se)


@coroutine
@without_document_lock
def get_new_data():
    doc.add_next_tick_callback(start_apply)
    results = yield EXECUTOR.submit(do_spark_computation, text_to_find.value)
    total_count, filtered_count, script_url_nl_n, script_url_nl_se, location_nl_n, location_nl_se = results
    doc.add_next_tick_callback(partial(
        update_results,
        total_count,
        filtered_count,
        # script_url_n=script_url_n,
        # script_url_se=script_url_se,
        # location_n=location_n,
        # location_se=location_se,
        script_url_nl_n=script_url_nl_n,
        script_url_nl_se=script_url_nl_se,
        location_nl_n=location_nl_n,
        location_nl_se=location_nl_se,
    ))


apply_button.on_click(get_new_data)  # noqa
doc.add_periodic_callback(periodic_task, 1000)
