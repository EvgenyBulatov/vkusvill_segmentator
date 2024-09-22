# vkusvill_segmentator | ДИНОЗАВРИКИ МИСИС

### Решение второго кейса по параллельной сборке заказаов от ВкусВилл Даркстора. 

### Идея

Необходимо ускорить процесс сборки заказа работником ДС. Для этого оптимизируем пуллинг заказов. Будем матчить заказы не по равному кол-ву товаров, а используем ограничение по суммарному кол-ву товаров в СЛ. Также обращаем внимание на кол-во категорий товаров в заказах. Товары схожих категорий лежат рядом -> сборщик проходит меньше кол-во точек при сборке СЛ.

Технологически решение представлено на spark-3.2 c применением библиотек для анализа данных

### Условия сегментации для возможности спулливания заказов 

- Суммарное кол-во строк в СЛ не превышает заданное на ДС
- Заказы поступили примерно в одно время (разница между заказами не более 40 минут)
- Особо крупные товары не попадают в спуленные СЛ
- Экспресс-заказы не попадают с спуленные СЛ
- Производится подбор по схожести категорий товаров, наполняющих каждый из потенциально-спуленных заказов
- Остальные заказы возвращаются к работе для обработки стандартным алгоритмом спулливания, работающим на данный момент 
