import MySQLdb
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

def load_words():
    words = []
    with open('words.txt') as word_file:
        words = set(word_file.read().split())
    db=MySQLdb.connect("localhost","root","","threading")
    insert = "INSERT INTO words (id, name, hits) VALUES "
    counter = 0
    for word in words:
        if counter > 0:
            insert += ", "
        counter += 1
        insert += "\r\n (0, '" + word + "', 0)"
    c=db.cursor()
    c.execute(insert + ";")
    db.commit()


if __name__ == '__main__':
    load_words()
