# QuotesPollingService API

### TODO:
Создать сервис получения данных по валютным парам с бирж: Bitfinex и Kraken

### API

База создаётся при первом запуске программы.
Если БД пустая, запросить и записать в БД данные по парам BTC/USD, ETH/USD, XRP/EUR, XRP/USD:
    • За каждый час ближайших 30 дней
    • За каждую минуту ближайших суток.
`По запросу пользователем одной из валютных пар, выбрать из базы(часовые данные) по два элемента каждого дня: 1) максимальный по параметру High; 2) минимальный по параметру Low. Вернуть в формате JSON:
[{
Type: “min”/”max”
time: [DateTime],
close: [number],
open: [number],
high: [number],
low: [number],
volume: [number],
Exchange: [text] //Имя биржи
}]`

https://docs.kraken.com/rest 

https://docs.bitfinex.com/reference 

#### requirements
- DB: SQLite, Databases(+aiosqlite)
- Web Framework: fastapi
- async Http Client: aiohttp
- pytest
- poetry
- docker, docker-compose ?