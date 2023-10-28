import MySQLdb
import logging
import logging.config
import logging.handlers
import syslog
import threading
import time
import sys
import random
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

class thread_master:
    def modify_words(self, thread, random):
        logging.debug('Thread ' + str(thread) + ' Started')
        syslog.syslog('Thread ' + str(thread) + ' Started')
        self.update_db()
        self.select_db()
        logging.debug('Thread ' + str(thread) + ' Finished')
        syslog.syslog('Thread ' + str(thread) + ' Finished')
        return

    def select_db(self):
        db=MySQLdb.connect("localhost","root","","threading")
        selectq = "SELECT * FROM words where IF(mod(id," + str(random.randint(1, 10)) + "),'t', 'f')  = 't';"
        print(selectq)
        c=db.cursor()
        c.execute(selectq)
        counter = 0
        words = c.fetchall()
        for word in words:
            counter += 1
        logging.debug('Fetched: ' + str(counter))
        syslog.syslog('Fetched: ' + str(counter))

    def update_db(self):
        db=MySQLdb.connect("localhost","root","","threading")
        selectq = "UPDATE words SET hits = hits + 1 where IF(mod(id," + str(random.randint(1, 10)) + "),'t', 'f')  = 't';"
        print(selectq)
        c=db.cursor()
        c.execute(selectq)
        db.commit()

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(threadName)s %(message)s')
        logging.debug('Allocating Worker Threads')
        syslog.syslog('Allocating Worker Threads')
        worker_threads = []
        try:
            for i in range(0, 20):
                x = threading.Thread(target=self.modify_words, args=(len(worker_threads), "Hello"))
                worker_threads.append(x)
            logging.debug('Finished allocating Worker Threads')
            syslog.syslog('Finished allocating Worker Threads')
            logging.debug('Starting Worker Threads')
            syslog.syslog('Starting Worker Threads')
            for w in worker_threads:
                w.start()
        except Exception as e:
            logging.debug(e)
            sys.exit(1)
        logging.debug('All Worker Threads started')
        syslog.syslog('All Worker Threads started')

if __name__ == '__main__':
    a = thread_master()
