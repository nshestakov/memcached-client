# memcached-client
Simple UDP memcached client

Тривиальная реализация memcached клиента работающего по UDP протоколу. Имеет смысл его использовать
если одновременно работа происходит с несколькими memcached находящихся в разлицных датацентрах и 
данные не имеют кричную важность.

Позволяет зеркалировать и шардировать данные на серверах.

Ограничения:
Размер ключа и данных должен умещаться в один UDP пакет (обычно это около 1400 байт)

