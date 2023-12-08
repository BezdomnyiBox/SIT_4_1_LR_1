import sqlite3
import pandas as pd
# создаем базу данных и устанавливаем соединение с ней
con = sqlite3.connect("booking.sqlite")
# открываем файл с дампом базой двнных
f_damp = open('booking.db','r', encoding ='utf-8-sig')
# читаем данные из файла
damp = f_damp.read()
# закрываем файл с дампом
f_damp.close()
# запускаем запросы
con.executescript(damp)
# сохраняем информацию в базе данных
con.commit()

#вывод на консоль
""" # создаем курсор
cursor = con.cursor()
# выбираем и выводим записи из таблиц room_booking, guest, room, status
cursor.execute('''
 SELECT
 guest_name,
 room_name,
 check_in_date,
 check_out_date - check_in_date + 1 AS Количество_дней
 FROM room_booking
 JOIN guest USING (guest_id)
 JOIN room USING (room_id)
 JOIN status USING (status_id)
 WHERE check_in_date >= :p1 AND check_in_date <= :p2 AND status_name = :p3
 ORDER BY guest_name ASC, room_name ASC, check_in_date DESC
 ''',{"p1": "2020-01-01", "p2": "2021-12-31", "p3": "Занят"})
print(cursor.fetchall()) """

#№1
#создание дата фрейма 
""" df1 = pd.read_sql('''
 SELECT
 guest_name AS ФИО,
 room_name AS Название_номера,
 check_in_date AS Дата_заселения,
 check_out_date - check_in_date + 1 AS Количество_дней
 FROM room_booking
 JOIN guest USING (guest_id)
 JOIN room USING (room_id)
 JOIN status USING (status_id)
 WHERE check_in_date >= :p1 AND check_in_date <= :p2 AND status_name = :p3
 ORDER BY guest_name ASC, room_name ASC, check_in_date DESC
''', con, params={"p1": "2020-01-01", "p2": "2021-12-31", "p3": "Занят"})
print(df1) """

#№2
""" df2 = pd.read_sql('''
 SELECT
    'Количество гостей' AS "Характеристика",
    COUNT(rb.guest_id) AS "Результат"
 FROM room_booking rb
 UNION ALL
 SELECT
    'Количество номеров' AS "Характеристика",
    COUNT(DISTINCT r.room_id) AS "Результат"
 FROM room r
 UNION ALL
 SELECT
    'Сумма за проживание' AS "Характеристика",
    SUM(tr.price) AS "Результат"
 FROM room_booking rb
 JOIN room r ON rb.room_id = r.room_id
 JOIN type_room tr ON r.type_room_id = tr.type_room_id
 UNION ALL
 SELECT
    'Количество услуг' AS "Характеристика",
    COUNT(*) AS "Результат"
 FROM service
 UNION ALL
 SELECT
    'Сумма за услуги' AS "Характеристика",
    SUM(sb.price) AS "Результат"
 FROM service_booking sb
''', con, params={})
print(df2) """

#№3
""" df3 = pd.read_sql('''
 SELECT
    tr.type_room_name AS Тип_номера,
    COUNT(rb.room_id) AS Количество,
    room.room_name AS Номера
 FROM room_booking rb
 JOIN status st ON rb.status_id = st.status_id
 JOIN room ON rb.room_id = room.room_id
 JOIN type_room tr ON room.type_room_id = tr.type_room_id
 WHERE status_name = :p1
 GROUP BY rb.room_id
 ORDER BY Тип_номера ASC, Номера DESC
''', con, params={"p1": "Занят"})
print(df3) """

#№4
# создаем курсор
cursor = con.cursor()
cursor.execute('''
 WITH NewCheckoutDate AS (
    SELECT
        rb.room_booking_id,
        rb.check_in_date,
        rb.check_out_date,
        rb.room_id,
        LEAD(rb.check_in_date, 1) OVER (PARTITION BY rb.room_id ORDER BY rb.check_in_date) AS next_check_in_date
    FROM
        room_booking rb
    WHERE
        rb.guest_id = 13
        AND rb.room_id = 34
        AND rb.check_in_date = '2020-12-15'
)
UPDATE room_booking
SET
    check_out_date = DATE(next_check_in_date, '+15 days')
FROM NewCheckoutDate
WHERE
    room_booking.room_booking_id = NewCheckoutDate.room_booking_id;
 ''',{})
print(cursor.fetchall())

#№5
""" df5 = pd.read_sql('''
 SELECT r.room_name
 FROM room r
 WHERE r.type_room_id = 4
 AND NOT EXISTS (
    SELECT 1
    FROM room_booking rb
    WHERE rb.room_id = r.room_id
    AND (
        (rb.check_in_date <= '2020-12-16' AND rb.check_out_date >= '2020-12-11')
        OR (rb.check_in_date <= '2021-02-16' AND rb.check_out_date >= '2021-02-11')
    )
 )
 ORDER BY r.room_name
 ''', con, params={})
print(df5) """
# закрываем соединение с базой
con.close()
