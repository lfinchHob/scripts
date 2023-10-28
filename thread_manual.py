import MySQLdb
import logging
import logging.config
import logging.handlers
import threading
import time
import sys
import random
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
"""
MariaDB [threading]> describe words;
+-------+--------------+------+-----+---------+----------------+
| Field | Type         | Null | Key | Default | Extra          |
+-------+--------------+------+-----+---------+----------------+
| id    | mediumint(9) | NO   | PRI | NULL    | auto_increment |
| name  | varchar(128) | NO   |     | NULL    |                |
| hits  | int(11)      | NO   |     | NULL    |                |
+-------+--------------+------+-----+---------+----------------+
"""
resource = Resource(attributes={
    SERVICE_NAME: "Thread Manual"
})

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://192.168.20.53:4318/v1/traces"))
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("Thread.Manual")
class thread_master:
    @tracer.start_as_current_span("modify words")
    def modify_words(self, thread):
        current_span = trace.get_current_span()
        current_span.add_event('Thread ' + str(thread) + ' Started')
        current_span.set_attribute("thread.number", str(thread))
        self.update_db()
        self.sleeper(random.randint(1, 10))
        self.select_db()
        current_span.add_event('Thread ' + str(thread) + ' Finished')
        return

    @tracer.start_as_current_span("Sleeper function")
    def sleeper(self, duration):
        current_span = trace.get_current_span()
        current_span.set_attribute("sleep.duration", str(duration))
        time.sleep(duration)

    @tracer.start_as_current_span("select_db")
    def select_db(self):
        db=MySQLdb.connect("localhost","root","","threading")
        selectq = "SELECT * FROM words where IF(mod(id," + str(random.randint(1, 10)) + "),'t', 'f')  = 't';"
        print(selectq)
        current_span = trace.get_current_span()
        current_span.set_attribute("query.text", selectq)
        c=db.cursor()
        c.execute(selectq)
        counter = 0
        words = c.fetchall()
        for word in words:
            counter += 1

    @tracer.start_as_current_span("update_db")
    def update_db(self):
        db=MySQLdb.connect("localhost","root","","threading")
        selectq = "UPDATE words SET hits = hits + 1 where IF(mod(id," + str(random.randint(1, 10)) + "),'t', 'f')  = 't';"
        print(selectq)
        current_span = trace.get_current_span()
        current_span.set_attribute("query.text", selectq)
        c=db.cursor()
        c.execute(selectq)
        db.commit()

    @tracer.start_as_current_span("init")
    def __init__(self):
        logging.debug('Allocating Worker Threads')
        worker_threads = []
        try:
            for i in range(0, 20):
                x = threading.Thread(target=self.modify_words, args=([len(worker_threads)]))
                worker_threads.append(x)
            for w in worker_threads:
                w.start()
        except Exception as e:
            sys.exit(1)

if __name__ == '__main__':
    a = thread_master()
