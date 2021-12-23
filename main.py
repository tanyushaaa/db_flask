import psycopg2
from flask import Flask
from flask import request, make_response, redirect, abort, render_template
from contextlib import closing
from psycopg2.sql import *

app = Flask(__name__)


class PGClient:
    def __init__(self):
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:  #conn.cursor(cursor_factory=DictCursor)
                # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                tab = ['MAIN', 'SURNAME', 'NAME', 'PATRONYMIC', 'STREET']
                self.tables = []
                for table in tab:
                    cursor.execute(SQL('Select * FROM {} LIMIT 0').format(Identifier(table)))
                    self.tables.append({'name': table,
                                        'columns': [desc[0] for desc in cursor.description]})
                                       # 'columns_count': len()}

                # print(self.tables)

    def select_main(self, searchable_user={}, id=0):
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:
                if searchable_user:
                    # print(searchable_user)
                    query = ''
                    params = []
                    if searchable_user['surname']:
                        if query:
                            query += ' AND '
                        else:
                            query += ' WHERE '
                        query += '{s_val} = %s'
                        params.append(searchable_user['surname'])
                    if searchable_user['name']:
                        if query:
                            query += ' AND '
                        else:
                            query += ' WHERE '
                        query += '{n_val} = %s'
                        params.append(searchable_user['name'])
                    if searchable_user['patronymic']:
                        if query:
                            query += ' AND '
                        else:
                            query += ' WHERE '
                        query += '{p_val} = %s'
                        params.append(searchable_user['patronymic'])
                    if searchable_user['street']:
                        if query:
                            query += ' AND '
                        else:
                            query += ' WHERE '
                        query += '{st_val} = %s'
                        params.append(searchable_user['street'])
                    query = SQL(query).format(s_val=Identifier('s_val'),
                                              n_val=Identifier('n_val'),
                                              p_val=Identifier('p_val'),
                                              st_val=Identifier('st_val'))
                    cursor.execute(SQL('''SELECT u_id, s_val, n_val, p_val, st_val, phone
                                      FROM public."MAIN"
                                      INNER
                                      JOIN public."SURNAME" ON surname = s_id
                                      JOIN public."NAME" ON name = n_id
                                      JOIN public."PATRONYMIC" ON patronymic = p_id
                                      JOIN public."STREET" ON street = st_id''') + query, params)
                    record = cursor.fetchall()
                elif id:
                    cursor.execute('''SELECT u_id, s_val, n_val, p_val, st_val, phone
                                      FROM public."MAIN"
                                      INNER
                                      JOIN public."SURNAME" ON surname = s_id
                                      JOIN public."NAME" ON name = n_id
                                      JOIN public."PATRONYMIC" ON patronymic = p_id
                                      JOIN public."STREET" ON street = st_id
                                      WHERE u_id= %s''', [id])
                    record = cursor.fetchall()
                else:  # SELECT u_id, s_val, n_val, p_val, st_val, bldn, kor, aprtm, phone
                    cursor.execute('''SELECT u_id, s_val, n_val, p_val, st_val, phone
                                      FROM public."MAIN"
                                      INNER
                                      JOIN public."SURNAME" ON surname = s_id
                                      JOIN public."NAME" ON name = n_id
                                      JOIN public."PATRONYMIC" ON patronymic = p_id
                                      JOIN public."STREET" ON street = st_id''')
                    record = cursor.fetchall()

                return record  # список кортежей

    def select(self, table, column):  #column = 0 or 1
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:  #conn.cursor(cursor_factory=DictCursor)
                cursor.execute(SQL('''SELECT {field} FROM {table};''')
                               .format(field=Identifier(table['columns'][column]),
                                       table=Identifier(table['name'])))
                record = cursor.fetchall()
                return record

    def insert_main(self, new_user):  #table = self.tables[i] - map, mb param - i? 1 - main
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:  #conn.cursor(cursor_factory=DictCursor)
                cursor.execute(SQL('''INSERT INTO {main_table} ({columns_names})
                                      VALUES(
                                      (SELECT {s_id} FROM {surname_table} 
                                      WHERE {s_val} = %s),
                                      (SELECT {n_id} FROM {name_table}
                                      WHERE {n_val} = %s),
                                      (SELECT {p_id} FROM {patronymic_table} 
                                      WHERE {p_val} = %s),
                                      (SELECT {st_id} FROM {street_table}
                                      WHERE {st_val} = %s),
                                      null, null, null, %s);''')
                               .format(main_table=Identifier(self.tables[0]['name']),
                                       columns_names=SQL(' ,').join(
                                           [Identifier(self.tables[0]['columns'][1]),
                                            Identifier(self.tables[0]['columns'][2]),
                                            Identifier(self.tables[0]['columns'][3]),
                                            Identifier(self.tables[0]['columns'][4]),
                                            Identifier(self.tables[0]['columns'][5]),
                                            Identifier(self.tables[0]['columns'][6]),
                                            Identifier(self.tables[0]['columns'][7]),
                                            Identifier(self.tables[0]['columns'][8])]),
                                       s_id=Identifier(self.tables[1]['columns'][0]),
                                       surname_table=Identifier(self.tables[1]['name']),
                                       s_val=Identifier(self.tables[1]['columns'][1]),
                                       s_val_read=Identifier(new_user['surname']),
                                       n_id=Identifier(self.tables[2]['columns'][0]),
                                       name_table=Identifier(self.tables[2]['name']),
                                       n_val=Identifier(self.tables[2]['columns'][1]),
                                       n_val_read=Identifier(new_user['name']),
                                       p_id=Identifier(self.tables[3]['columns'][0]),
                                       patronymic_table=Identifier(self.tables[3]['name']),
                                       p_val=Identifier(self.tables[3]['columns'][1]),
                                       p_val_read=Identifier(new_user['patronymic']),
                                       st_id=Identifier(self.tables[4]['columns'][0]),
                                       street_table=Identifier(self.tables[4]['name']),
                                       st_val=Identifier(self.tables[4]['columns'][1]),
                                       st_val_read=Identifier(new_user['street']),
                                       ), [new_user['surname'], new_user['name'], new_user['patronymic'],
                                           new_user['street'], new_user['phone']])

                conn.commit()

    def delete(self, id):
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:  #conn.cursor(cursor_factory=DictCursor)
                cursor.execute('''DELETE FROM "MAIN" WHERE u_id = %s;''', [id])
                # record = cursor.fetchall()
            conn.commit()

    def update_main(self, id, new_data):
        with closing(psycopg2.connect(dbname='telephone_manager', user='postgres',
                                      password='513230', host='localhost')) as conn:
            with conn.cursor() as cursor:  #conn.cursor(cursor_factory=DictCursor)
                cursor.execute('''UPDATE "MAIN" 
                                  SET surname = (SELECT s_id FROM "SURNAME" WHERE s_val = %s),
                                  name = (SELECT n_id FROM "NAME" WHERE n_val = %s),
                                  patronymic = (SELECT p_id FROM "PATRONYMIC" WHERE p_val = %s),
                                  street = (SELECT st_id FROM "STREET" WHERE st_val = %s),
                                  phone = %s
                                  WHERE u_id = %s;''', [new_data['surname'], new_data['name'],
                                                        new_data['patronymic'], new_data['street'],
                                                        new_data['phone'], id])
                # record = cursor.fetchall()
            conn.commit()


columns = {'u_id': 0,
           'surname': 1,
           'name': 2,
           'patronymic': 3,
           'street': 4,
           'house': 5,
           'building': 6,
           'flat': 7,
           'number': 8}

client = PGClient()
parent_tables = {}
for table in client.tables:
    if table['name'] != 'MAIN':
        parent_tables[table['name']] = client.select(table, 1)


@app.route('/', methods=['post', 'get'])
def index():
    values = {'surname': '', 'name': '', 'patronymic': '', 'street': '', 'phone': ''}
    for value in values:
        values[value] = request.form.get(value)
    if request.method == 'POST':
        if request.form['button'] == 'Добавить':
            client.insert_main(values)
        elif request.form['button'] == 'Найти':
            return render_template('indexxx.html',
                                   users_list=client.select_main(values),
                                   columns=columns,
                                   drop_lists=parent_tables)

    return render_template('indexxx.html', users_list=client.select_main(), columns=columns, drop_lists=parent_tables)


@app.route('/d<u_id>')
def deleting(u_id):
    client.delete(u_id)
    return redirect('/')


@app.route('/<u_id>', methods=['post', 'get'])
def contact_page(u_id):
    values = {'surname': '', 'name': '', 'patronymic': '', 'street': '', 'phone': ''}
    contact = client.select_main(id=u_id)[0]
    i = 1
    for key in values:
        values[key] = contact[i]
        i += 1
    if request.method == 'POST':
        for param in values:
            values[param] = request.form.get(param)
        client.update_main(id=u_id, new_data=values)
    return render_template('user_page.html', id=u_id, contact=values, drop_lists=parent_tables)


@app.route('/settings')
def settings_page():
    return render_template('settings.html', tables=parent_tables)


@app.route('/settings/<table>', methods=['post', 'get'])
def setting_table(table):
    for tab in client.tables:
        if tab['name'] == table.upper():
            table = tab
            break
    if request.method == 'POST':
        if request.form['button'] == 'Добавить':
            pass
        if request.form['button'] == 'Найти':
            pass
        if request.form['button'] == 'Изменить':
            pass
        if request.form['button'] == 'Удалить':
            pass

    return render_template('setting_table.html', table=table, id_list=client.select(table, 0), val_list=client.select(table, 1))


if __name__ == '__main__':
    app.run(debug=True)
